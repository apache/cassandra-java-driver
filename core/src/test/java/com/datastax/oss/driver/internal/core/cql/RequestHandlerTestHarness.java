/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metadata.LoadBalancingPolicyWrapper;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.datastax.oss.driver.internal.core.util.concurrent.ScheduledTaskCapturingEventLoop;
import com.datastax.oss.protocol.internal.Frame;
import io.netty.channel.EventLoopGroup;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.OngoingStubbing;

/**
 * Provides the environment to test a request handler, where a query plan can be defined, and the
 * behavior of each successive node simulated.
 */
public class RequestHandlerTestHarness implements AutoCloseable {

  public static Builder builder() {
    return new Builder();
  }

  private final ScheduledTaskCapturingEventLoop schedulingEventLoop;

  @Mock private InternalDriverContext context;
  @Mock private DefaultSession session;
  @Mock private EventLoopGroup eventLoopGroup;
  @Mock private NettyOptions nettyOptions;
  @Mock private DriverConfig config;
  @Mock private DriverConfigProfile defaultConfigProfile;
  @Mock private LoadBalancingPolicyWrapper loadBalancingPolicyWrapper;
  @Mock private RetryPolicy retryPolicy;
  @Mock private SpeculativeExecutionPolicy speculativeExecutionPolicy;

  private RequestHandlerTestHarness(Builder builder) {
    MockitoAnnotations.initMocks(this);

    this.schedulingEventLoop = new ScheduledTaskCapturingEventLoop(eventLoopGroup);
    Mockito.when(eventLoopGroup.next()).thenReturn(schedulingEventLoop);
    Mockito.when(nettyOptions.ioEventLoopGroup()).thenReturn(eventLoopGroup);
    Mockito.when(context.nettyOptions()).thenReturn(nettyOptions);

    // TODO make configurable in the test, also handle profiles
    Mockito.when(defaultConfigProfile.getDuration(CoreDriverOption.REQUEST_TIMEOUT))
        .thenReturn(Duration.ofMillis(500));
    Mockito.when(defaultConfigProfile.getConsistencyLevel(CoreDriverOption.REQUEST_CONSISTENCY))
        .thenReturn(ConsistencyLevel.LOCAL_ONE);
    Mockito.when(defaultConfigProfile.getInt(CoreDriverOption.REQUEST_PAGE_SIZE)).thenReturn(5000);
    Mockito.when(
            defaultConfigProfile.getConsistencyLevel(CoreDriverOption.REQUEST_SERIAL_CONSISTENCY))
        .thenReturn(ConsistencyLevel.SERIAL);
    Mockito.when(defaultConfigProfile.getBoolean(CoreDriverOption.REQUEST_DEFAULT_IDEMPOTENCE))
        .thenReturn(builder.defaultIdempotence);

    Mockito.when(config.defaultProfile()).thenReturn(defaultConfigProfile);
    Mockito.when(context.config()).thenReturn(config);

    Mockito.when(loadBalancingPolicyWrapper.newQueryPlan()).thenReturn(builder.buildQueryPlan());
    Mockito.when(context.loadBalancingPolicyWrapper()).thenReturn(loadBalancingPolicyWrapper);

    Mockito.when(context.retryPolicy()).thenReturn(retryPolicy);
    Mockito.when(context.speculativeExecutionPolicy()).thenReturn(speculativeExecutionPolicy);

    Mockito.when(context.codecRegistry()).thenReturn(new DefaultCodecRegistry("test"));

    Map<Node, ChannelPool> pools = builder.buildMockPools();
    Mockito.when(session.getPools()).thenReturn(pools);
    Mockito.when(session.getRepreparePayloads()).thenReturn(new ConcurrentHashMap<>());
  }

  public DefaultSession getSession() {
    return session;
  }

  public InternalDriverContext getContext() {
    return context;
  }

  /**
   * Returns the next task that was scheduled on the request handler's admin executor. The test must
   * run it manually.
   */
  public ScheduledTaskCapturingEventLoop.CapturedTask<?> nextScheduledTask() {
    return schedulingEventLoop.nextTask();
  }

  @Override
  public void close() {
    schedulingEventLoop.shutdownGracefully().getNow();
  }

  public static class Builder {
    private final List<PoolBehavior> poolBehaviors = new ArrayList<>();
    private boolean defaultIdempotence;

    /**
     * Sets the given node as the next one in the query plan; an empty pool will be simulated when
     * it gets used.
     */
    public Builder withEmptyPool(Node node) {
      poolBehaviors.add(new PoolBehavior(node, false));
      return this;
    }

    /**
     * Sets the given node as the next one in the query plan; a channel write failure will be
     * simulated when it gets used.
     */
    public Builder withWriteFailure(Node node, Throwable cause) {
      PoolBehavior behavior = new PoolBehavior(node, true);
      behavior.setWriteFailure(cause);
      poolBehaviors.add(behavior);
      return this;
    }

    /**
     * Sets the given node as the next one in the query plan; the write to the channel will succeed,
     * but a response failure will be simulated immediately after.
     */
    public Builder withResponseFailure(Node node, Throwable cause) {
      PoolBehavior behavior = new PoolBehavior(node, true);
      behavior.setWriteSuccess();
      behavior.setResponseFailure(cause);
      poolBehaviors.add(behavior);
      return this;
    }

    /**
     * Sets the given node as the next one in the query plan; the write to the channel will succeed,
     * and the given response will be simulated immediately after.
     */
    public Builder withResponse(Node node, Frame response) {
      PoolBehavior behavior = new PoolBehavior(node, true);
      behavior.setWriteSuccess();
      behavior.setResponseSuccess(response);
      poolBehaviors.add(behavior);
      return this;
    }

    public Builder withDefaultIdempotence(boolean defaultIdempotence) {
      this.defaultIdempotence = defaultIdempotence;
      return this;
    }

    /**
     * Sets the given node as the next one in the query plan; the test code is responsible of
     * calling the methods on the returned object to complete the write and the query.
     */
    public PoolBehavior customBehavior(Node node) {
      PoolBehavior behavior = new PoolBehavior(node, true);
      poolBehaviors.add(behavior);
      return behavior;
    }

    public RequestHandlerTestHarness build() {
      return new RequestHandlerTestHarness(this);
    }

    private Queue<Node> buildQueryPlan() {
      ConcurrentLinkedQueue<Node> queryPlan = new ConcurrentLinkedQueue<>();
      for (PoolBehavior behavior : poolBehaviors) {
        // We don't want duplicates in the query plan: the only way a node is tried multiple times
        // is if the retry policy returns a RETRY_SAME, the request handler does not re-read from
        // the plan.
        if (!queryPlan.contains(behavior.node)) {
          queryPlan.offer(behavior.node);
        }
      }
      return queryPlan;
    }

    private Map<Node, ChannelPool> buildMockPools() {
      Map<Node, ChannelPool> pools = new ConcurrentHashMap<>();
      Map<Node, OngoingStubbing<DriverChannel>> stubbings = new HashMap<>();
      for (PoolBehavior behavior : poolBehaviors) {
        Node node = behavior.node;
        ChannelPool pool = pools.computeIfAbsent(node, n -> Mockito.mock(ChannelPool.class));

        // The goal of the code below is to generate the equivalent of:
        //
        //     Mockito.when(pool.next())
        //       .thenReturn(behavior1.channel)
        //       .thenReturn(behavior2.channel)
        //       ...
        stubbings.compute(
            node,
            (sameNode, previous) -> {
              if (previous == null) {
                previous = Mockito.when(pool.next());
              }
              return previous.thenReturn(behavior.channel);
            });
      }
      return pools;
    }
  }
}
