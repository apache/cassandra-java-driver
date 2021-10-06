package com.yugabyte.oss.driver.internal.core.loadbalancing;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.loadbalancing.BasicLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Predicate;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YugabyteDefaultLoadBalancingPolicy extends BasicLoadBalancingPolicy
    implements RequestTracker {

  private static final Logger LOG =
      LoggerFactory.getLogger(YugabyteDefaultLoadBalancingPolicy.class);

  private volatile DistanceReporter distanceReporter;
  private volatile Predicate<Node> filter;
  private volatile String localDc;

  protected final CopyOnWriteArraySet<Node> localDCliveNodes = new CopyOnWriteArraySet<>();
  protected final Map<Node, AtomicLongArray> responseTimes = new ConcurrentHashMap<>();

  public YugabyteDefaultLoadBalancingPolicy(DriverContext context, String profileName) {
    super(context, profileName);
  }

  @Override
  public void init(@NonNull Map<UUID, Node> nodes, @NonNull DistanceReporter distanceReporter) {
    this.distanceReporter = distanceReporter;
    localDc = discoverLocalDc(nodes).orElse(null);
    filter = createNodeFilter(localDc, nodes);

    for (Node node : nodes.values()) {
      isLiveNode(node);
    }
  }

  @NonNull
  @Override
  public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session) {
    // Take a snapshot since the set is concurrent:
    Object[] currentNodes = null;

    if (!StringUtils.isBlank(localDc)) {

      currentNodes = localDCliveNodes.toArray();
      // if there are no healthy nodes in the localDC, then consider nodes from other
      // DCs.
      if (currentNodes.length == 0) {

        LOG.trace(
            "[{}] No nodes available in Local DC {}, falling back on to liveNodes",
            logPrefix,
            localDc);
        currentNodes = liveNodes.toArray();
      }

    } else {
      currentNodes = liveNodes.toArray();
    }

    LOG.trace("[{}] Round-robing the {} avaiable nodes", logPrefix, currentNodes.length);

    // Round-robin the remaining nodes
    ArrayUtils.rotate(
        currentNodes, 0, currentNodes.length, roundRobinAmount.getAndUpdate(INCREMENT));

    return new QueryPlan(currentNodes);
  }

  @Override
  public void onUp(@NonNull Node node) {
    isLiveNode(node);
  }

  @Override
  public void onAdd(@NonNull Node node) {
    isLiveNode(node);
  }

  @Override
  public void onNodeSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    updateResponseTimes(node);
  }

  @Override
  public void onNodeError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    updateResponseTimes(node);
  }

  private void isLiveNode(@NonNull Node node) {

    // For YCQL, when localDC is provided, use the DefaultNodeFilterHelper to find
    // out the
    // local nodes.
    if (!StringUtils.isBlank(localDc)) {

      if (filter.test(node)) {
        distanceReporter.setDistance(node, NodeDistance.LOCAL);
        if (node.getState() != NodeState.DOWN) {
          // This includes state == UNKNOWN. If the node turns out to be unreachable, this
          // will be
          // detected when we try to open a pool to it, it will get marked down and this
          // will be
          // signaled back to this policy
          localDCliveNodes.add(node);
        }
      } else {

        // For Multi-DC/Multi-region YugabyteDB Clusters, leader tablets can be present
        // in the DC's
        // other than the DC considered as LOCAL. Hence we'll need to consider the nodes
        // from
        // other DC's as live for routing the YCQL operations.
        if (node.getState() == NodeState.UP || node.getState() == NodeState.UNKNOWN) {
          liveNodes.add(node);
          LOG.debug(
              "[{}] Adding {} as it belongs to the {} DC in Multi-DC/Region Setup",
              logPrefix,
              node,
              node.getDatacenter());
          distanceReporter.setDistance(node, NodeDistance.REMOTE);
        } else {
          distanceReporter.setDistance(node, NodeDistance.IGNORED);
        }
      }
    } else {
      // For YCQL, When Local DC is not provided, all the available UP nodes will be
      // considered
      // local
      distanceReporter.setDistance(node, NodeDistance.LOCAL);
      if (node.getState() != NodeState.DOWN) {
        // This includes state == UNKNOWN. If the node turns out to be unreachable, this
        // will be
        // detected when we try to open a pool to it, it will get marked down and this
        // will be
        // signaled back to this policy
        liveNodes.add(node);
      }
    }
  }

  protected void updateResponseTimes(@NonNull Node node) {
    responseTimes.compute(
        node,
        (n, array) -> {
          // The array stores at most two timestamps, since we don't need more;
          // the first one is always the least recent one, and hence the one to inspect.
          long now = nanoTime();
          if (array == null) {
            array = new AtomicLongArray(1);
            array.set(0, now);
          } else if (array.length() == 1) {
            long previous = array.get(0);
            array = new AtomicLongArray(2);
            array.set(0, previous);
            array.set(1, now);
          } else {
            array.set(0, array.get(1));
            array.set(1, now);
          }
          return array;
        });
  }

  protected long nanoTime() {
    return System.nanoTime();
  }

  // private void addToLiveNodes(@NonNull Node node) {
  //
  // String dc = node.getDatacenter();
  // if (dc == null) {
  // LOG.trace("[{}] Datacenter value not avaiable for node {}", logPrefix,
  // node.getEndPoint());
  // }
  //
  // CopyOnWriteArrayList<Node> prev = perDcLiveNodes.get(dc);
  // if (prev == null)
  // perDcLiveNodes.put(dc, new
  // CopyOnWriteArrayList<Node>(Collections.singletonList(node)));
  // else
  // prev.addIfAbsent(node);
  // }

}
