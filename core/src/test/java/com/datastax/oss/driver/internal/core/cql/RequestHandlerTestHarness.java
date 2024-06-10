/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.cql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.DefaultConsistencyLevelRegistry;
import com.datastax.oss.driver.internal.core.ProtocolFeature;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.LoadBalancingPolicyWrapper;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.servererrors.DefaultWriteTypeRegistry;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.throttling.PassThroughRequestThrottler;
import com.datastax.oss.driver.internal.core.tracker.NoopRequestTracker;
import com.datastax.oss.driver.internal.core.util.concurrent.CapturingTimer;
import com.datastax.oss.driver.internal.core.util.concurrent.CapturingTimer.CapturedTimeout;
import com.datastax.oss.protocol.internal.Frame;
import io.netty.channel.EventLoopGroup;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.mockito.Mock;
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

  private final CapturingTimer timer = new CapturingTimer();
  private final Map<Node, ChannelPool> pools;

  @Mock protected InternalDriverContext context;
  @Mock protected DefaultSession session;
  @Mock protected EventLoopGroup eventLoopGroup;
  @Mock protected NettyOptions nettyOptions;
  @Mock protected DriverConfig config;
  @Mock protected DriverExecutionProfile defaultProfile;
  @Mock protected LoadBalancingPolicyWrapper loadBalancingPolicyWrapper;
  @Mock protected RetryPolicy retryPolicy;
  @Mock protected SpeculativeExecutionPolicy speculativeExecutionPolicy;
  @Mock protected TimestampGenerator timestampGenerator;
  @Mock protected ProtocolVersionRegistry protocolVersionRegistry;
  @Mock protected SessionMetricUpdater sessionMetricUpdater;

  protected RequestHandlerTestHarness(Builder builder) {
    MockitoAnnotations.initMocks(this);

    when(nettyOptions.getTimer()).thenReturn(timer);
    when(nettyOptions.ioEventLoopGroup()).thenReturn(eventLoopGroup);
    when(context.getNettyOptions()).thenReturn(nettyOptions);

    when(defaultProfile.getName()).thenReturn(DriverExecutionProfile.DEFAULT_NAME);
    // TODO make configurable in the test, also handle profiles
    when(defaultProfile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
        .thenReturn(Duration.ofMillis(500));
    when(defaultProfile.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .thenReturn(DefaultConsistencyLevel.LOCAL_ONE.name());
    when(defaultProfile.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE)).thenReturn(5000);
    when(defaultProfile.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
        .thenReturn(DefaultConsistencyLevel.SERIAL.name());
    when(defaultProfile.getBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE))
        .thenReturn(builder.defaultIdempotence);
    when(defaultProfile.getBoolean(DefaultDriverOption.PREPARE_ON_ALL_NODES)).thenReturn(true);

    when(config.getDefaultProfile()).thenReturn(defaultProfile);
    when(context.getConfig()).thenReturn(config);

    when(loadBalancingPolicyWrapper.newQueryPlan(
            any(Request.class), anyString(), any(Session.class)))
        .thenReturn(builder.buildQueryPlan());
    when(context.getLoadBalancingPolicyWrapper()).thenReturn(loadBalancingPolicyWrapper);

    when(context.getRetryPolicy(anyString())).thenReturn(retryPolicy);

    // Disable speculative executions by default
    when(speculativeExecutionPolicy.nextExecution(
            any(Node.class), any(CqlIdentifier.class), any(Request.class), anyInt()))
        .thenReturn(-1L);
    when(context.getSpeculativeExecutionPolicy(anyString())).thenReturn(speculativeExecutionPolicy);

    when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);

    when(timestampGenerator.next()).thenReturn(Statement.NO_DEFAULT_TIMESTAMP);
    when(context.getTimestampGenerator()).thenReturn(timestampGenerator);

    pools = builder.buildMockPools();
    when(session.getChannel(any(Node.class), anyString(), any()))
        .thenAnswer(
            invocation -> {
              Node node = invocation.getArgument(0);
              return pools.get(node).next();
            });
    when(session.getChannel(any(Node.class), anyString()))
        .thenAnswer(
            invocation -> {
              Node node = invocation.getArgument(0);
              return pools.get(node).next();
            });
    when(session.getRepreparePayloads()).thenReturn(new ConcurrentHashMap<>());

    when(session.setKeyspace(any(CqlIdentifier.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    when(session.getMetricUpdater()).thenReturn(sessionMetricUpdater);
    when(sessionMetricUpdater.isEnabled(any(SessionMetric.class), anyString())).thenReturn(true);

    when(session.getMetadata()).thenReturn(DefaultMetadata.EMPTY);

    when(context.getProtocolVersionRegistry()).thenReturn(protocolVersionRegistry);
    when(protocolVersionRegistry.supports(any(ProtocolVersion.class), any(ProtocolFeature.class)))
        .thenReturn(true);

    if (builder.protocolVersion != null) {
      when(context.getProtocolVersion()).thenReturn(builder.protocolVersion);
    }

    when(context.getConsistencyLevelRegistry()).thenReturn(new DefaultConsistencyLevelRegistry());

    when(context.getWriteTypeRegistry()).thenReturn(new DefaultWriteTypeRegistry());

    when(context.getRequestThrottler()).thenReturn(new PassThroughRequestThrottler(context));

    when(context.getRequestTracker()).thenReturn(new NoopRequestTracker(context));
  }

  public DefaultSession getSession() {
    return session;
  }

  public InternalDriverContext getContext() {
    return context;
  }

  public DriverChannel getChannel(Node node) {
    ChannelPool pool = pools.get(node);
    return pool.next();
  }

  /**
   * Returns the next task that was scheduled on the request handler's admin executor. The test must
   * run it manually.
   */
  public CapturedTimeout nextScheduledTimeout() {
    return timer.getNextTimeout();
  }

  @Override
  public void close() {
    timer.stop();
  }

  public static class Builder {
    private final List<PoolBehavior> poolBehaviors = new ArrayList<>();
    private boolean defaultIdempotence;
    private ProtocolVersion protocolVersion;

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

    public Builder withProtocolVersion(ProtocolVersion protocolVersion) {
      this.protocolVersion = protocolVersion;
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
        ChannelPool pool = pools.computeIfAbsent(node, n -> mock(ChannelPool.class));

        // The goal of the code below is to generate the equivalent of:
        //
        //     when(pool.next())
        //       .thenReturn(behavior1.channel)
        //       .thenReturn(behavior2.channel)
        //       ...
        stubbings.compute(
            node,
            (sameNode, previous) -> {
              if (previous == null) {
                previous = when(pool.next());
              }
              return previous.thenReturn(behavior.channel);
            });
      }
      return pools;
    }
  }
}
