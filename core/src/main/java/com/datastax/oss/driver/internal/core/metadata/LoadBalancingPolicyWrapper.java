/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.concurrent.ReplayingEventFilter;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps the user-provided LBPs for internal use. This serves multiple purposes:
 *
 * <ul>
 *   <li>help enforce the guarantee that init is called exactly once, and before any other method.
 *   <li>handle the early stages of initialization (before first actual connect), where the LBPs are
 *       not ready yet.
 *   <li>handle incoming node state events from the outside world and propagate them to the
 *       policies.
 *   <li>process distance decisions from the policies and propagate them to the outside world.
 * </ul>
 */
@ThreadSafe
public class LoadBalancingPolicyWrapper implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(LoadBalancingPolicyWrapper.class);

  private enum State {
    BEFORE_INIT,
    DURING_INIT,
    RUNNING,
    CLOSING
  }

  private final InternalDriverContext context;
  private final Set<LoadBalancingPolicy> policies;
  private final Map<String, LoadBalancingPolicy> policiesPerProfile;
  private final Map<LoadBalancingPolicy, SinglePolicyDistanceReporter> reporters;

  private final Lock distancesLock = new ReentrantLock();

  // Remember which distance each policy reported for each node. We assume that distance events will
  // be rare, so don't try to be too clever, a global lock should suffice.
  @GuardedBy("distancesLock")
  private final Map<Node, Map<LoadBalancingPolicy, NodeDistance>> distances;

  private final String logPrefix;
  private final ReplayingEventFilter<NodeStateEvent> eventFilter =
      new ReplayingEventFilter<>(this::processNodeStateEvent);
  private final AtomicReference<State> stateRef = new AtomicReference<>(State.BEFORE_INIT);

  public LoadBalancingPolicyWrapper(
      @NonNull InternalDriverContext context,
      @NonNull Map<String, LoadBalancingPolicy> policiesPerProfile) {
    this.context = context;

    this.policiesPerProfile = policiesPerProfile;
    ImmutableMap.Builder<LoadBalancingPolicy, SinglePolicyDistanceReporter> reportersBuilder =
        ImmutableMap.builder();
    // ImmutableMap.values does not remove duplicates, do it now so that we won't invoke a policy
    // more than once if it's associated with multiple profiles
    for (LoadBalancingPolicy policy : ImmutableSet.copyOf(policiesPerProfile.values())) {
      reportersBuilder.put(policy, new SinglePolicyDistanceReporter(policy));
    }
    this.reporters = reportersBuilder.build();
    // Just an alias to make the rest of the code more readable
    this.policies = reporters.keySet();

    this.distances = new HashMap<>();

    this.logPrefix = context.getSessionName();
    context.getEventBus().register(NodeStateEvent.class, this::onNodeStateEvent);
  }

  public void init() {
    if (stateRef.compareAndSet(State.BEFORE_INIT, State.DURING_INIT)) {
      LOG.debug("[{}] Initializing policies", logPrefix);
      // State events can happen concurrently with init, so we must record them and replay once the
      // policy is initialized.
      eventFilter.start();
      MetadataManager metadataManager = context.getMetadataManager();
      Metadata metadata = metadataManager.getMetadata();
      for (LoadBalancingPolicy policy : policies) {
        policy.init(metadata.getNodes(), reporters.get(policy));
      }
      if (stateRef.compareAndSet(State.DURING_INIT, State.RUNNING)) {
        eventFilter.markReady();
      } else { // closed during init
        assert stateRef.get() == State.CLOSING;
        for (LoadBalancingPolicy policy : policies) {
          policy.close();
        }
      }
    }
  }

  /**
   * Note: we could infer the profile name from the request again in this method, but since that's
   * already done in request processors, pass the value directly.
   *
   * @see LoadBalancingPolicy#newQueryPlan(Request, Session)
   */
  @NonNull
  public Queue<Node> newQueryPlan(
      @Nullable Request request, @NonNull String executionProfileName, @Nullable Session session) {
    switch (stateRef.get()) {
      case BEFORE_INIT:
      case DURING_INIT:
        // The contact points are not stored in the metadata yet:
        List<Node> nodes = new ArrayList<>(context.getMetadataManager().getContactPoints());
        Collections.shuffle(nodes);
        return new ConcurrentLinkedQueue<>(nodes);
      case RUNNING:
        LoadBalancingPolicy policy = policiesPerProfile.get(executionProfileName);
        if (policy == null) {
          policy = policiesPerProfile.get(DriverExecutionProfile.DEFAULT_NAME);
        }
        return policy.newQueryPlan(request, session);
      default:
        return new ConcurrentLinkedQueue<>();
    }
  }

  @NonNull
  public Queue<Node> newQueryPlan() {
    return newQueryPlan(null, DriverExecutionProfile.DEFAULT_NAME, null);
  }

  // when it comes in from the outside
  private void onNodeStateEvent(NodeStateEvent event) {
    eventFilter.accept(event);
  }

  // once it has gone through the filter
  private void processNodeStateEvent(NodeStateEvent event) {
    switch (stateRef.get()) {
      case BEFORE_INIT:
      case DURING_INIT:
        throw new AssertionError("Filter should not be marked ready until LBP init");
      case CLOSING:
        return; // ignore
      case RUNNING:
        for (LoadBalancingPolicy policy : policies) {
          if (event.newState == NodeState.UP) {
            policy.onUp(event.node);
          } else if (event.newState == NodeState.DOWN || event.newState == NodeState.FORCED_DOWN) {
            policy.onDown(event.node);
          } else if (event.newState == NodeState.UNKNOWN) {
            policy.onAdd(event.node);
          } else if (event.newState == null) {
            policy.onRemove(event.node);
          } else {
            LOG.warn("[{}] Unsupported event: {}", logPrefix, event);
          }
        }
        break;
    }
  }

  @Override
  public void close() {
    State old;
    while (true) {
      old = stateRef.get();
      if (old == State.CLOSING) {
        return; // already closed
      } else if (stateRef.compareAndSet(old, State.CLOSING)) {
        break;
      }
    }
    // If BEFORE_INIT, no need to close because they were never initialized
    // If DURING_INIT, this will be handled in init()
    if (old == State.RUNNING) {
      for (LoadBalancingPolicy policy : policies) {
        policy.close();
      }
    }
  }

  // An individual distance reporter for one of the policies. The results are aggregated across all
  // policies, the smallest distance for each node is used.
  private class SinglePolicyDistanceReporter implements LoadBalancingPolicy.DistanceReporter {

    private final LoadBalancingPolicy policy;

    private SinglePolicyDistanceReporter(LoadBalancingPolicy policy) {
      this.policy = policy;
    }

    @Override
    public void setDistance(@NonNull Node node, @NonNull NodeDistance suggestedDistance) {
      LOG.debug(
          "[{}] {} suggested {} to {}, checking what other policies said",
          logPrefix,
          policy,
          node,
          suggestedDistance);
      distancesLock.lock();
      try {
        Map<LoadBalancingPolicy, NodeDistance> distancesForNode =
            distances.computeIfAbsent(node, (n) -> new HashMap<>());
        distancesForNode.put(policy, suggestedDistance);
        NodeDistance newDistance = aggregate(distancesForNode);
        LOG.debug("[{}] Shortest distance across all policies is {}", logPrefix, newDistance);

        // There is a small race condition here (check-then-act on a volatile field). However this
        // would only happen if external code changes the distance, which is unlikely (and
        // dangerous).
        // The driver internals only ever set the distance here, and we're protected by the lock.
        NodeDistance oldDistance = node.getDistance();
        if (!oldDistance.equals(newDistance)) {
          LOG.debug("[{}] {} was {}, changing to {}", logPrefix, node, oldDistance, newDistance);
          DefaultNode defaultNode = (DefaultNode) node;
          defaultNode.distance = newDistance;
          context.getEventBus().fire(new DistanceEvent(newDistance, defaultNode));
        } else {
          LOG.debug("[{}] {} was already {}, ignoring", logPrefix, node, oldDistance);
        }
      } finally {
        distancesLock.unlock();
      }
    }

    private NodeDistance aggregate(Map<LoadBalancingPolicy, NodeDistance> distances) {
      NodeDistance minimum = NodeDistance.IGNORED;
      for (NodeDistance candidate : distances.values()) {
        if (candidate.compareTo(minimum) < 0) {
          minimum = candidate;
        }
      }
      return minimum;
    }
  }
}
