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
package com.datastax.dse.driver.internal.core.graph;

import static org.mockito.Mockito.when;

import com.datastax.dse.driver.DseTestFixtures;
import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.DefaultConsistencyLevelRegistry;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.cql.RequestHandlerTestHarness;
import com.datastax.oss.driver.internal.core.servererrors.DefaultWriteTypeRegistry;
import com.datastax.oss.driver.internal.core.session.throttling.PassThroughRequestThrottler;
import com.datastax.oss.driver.internal.core.tracker.NoopRequestTracker;
import com.datastax.oss.protocol.internal.Frame;
import io.netty.channel.EventLoop;
import java.time.Duration;
import javax.annotation.Nullable;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;

/**
 * Provides the environment to test a request handler, where a query plan can be defined, and the
 * behavior of each successive node simulated.
 */
public class GraphRequestHandlerTestHarness extends RequestHandlerTestHarness {

  @Mock DriverExecutionProfile testProfile;

  @Mock DriverExecutionProfile systemQueryExecutionProfile;

  @Mock DefaultDriverContext dseDriverContext;

  @Mock EventLoop eventLoop;

  protected GraphRequestHandlerTestHarness(
      Builder builder,
      @Nullable GraphProtocol graphProtocolForTestConfig,
      Duration graphTimeout,
      @Nullable Version dseVersionForTestMetadata) {
    super(builder);

    // not mocked by RequestHandlerTestHarness, will be used when DseDriverOptions.GRAPH_TIMEOUT
    // is not zero in the config
    when(eventLoopGroup.next()).thenReturn(eventLoop);

    // default graph options as in the reference.conf file
    when(defaultProfile.getString(DseDriverOption.GRAPH_TRAVERSAL_SOURCE, null)).thenReturn("g");
    when(defaultProfile.isDefined(DseDriverOption.GRAPH_SUB_PROTOCOL)).thenReturn(Boolean.FALSE);
    when(defaultProfile.getBoolean(DseDriverOption.GRAPH_IS_SYSTEM_QUERY, false)).thenReturn(false);
    when(defaultProfile.getString(DseDriverOption.GRAPH_NAME, null)).thenReturn("mockGraph");
    when(defaultProfile.getDuration(DseDriverOption.GRAPH_TIMEOUT, Duration.ZERO))
        .thenReturn(graphTimeout);

    when(testProfile.getName()).thenReturn("test-graph");
    when(testProfile.getDuration(DseDriverOption.GRAPH_TIMEOUT, Duration.ZERO))
        .thenReturn(Duration.ofMillis(2L));
    when(testProfile.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .thenReturn(DefaultConsistencyLevel.LOCAL_ONE.name());
    when(testProfile.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE)).thenReturn(5000);
    when(testProfile.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
        .thenReturn(DefaultConsistencyLevel.SERIAL.name());
    when(testProfile.getBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE)).thenReturn(false);
    when(testProfile.getBoolean(DefaultDriverOption.PREPARE_ON_ALL_NODES)).thenReturn(true);
    when(testProfile.getString(DseDriverOption.GRAPH_TRAVERSAL_SOURCE, null)).thenReturn("a");
    when(testProfile.isDefined(DseDriverOption.GRAPH_SUB_PROTOCOL))
        .thenReturn(graphProtocolForTestConfig != null);
    // only mock the config if graphProtocolForTestConfig is not null
    if (graphProtocolForTestConfig != null) {
      when(testProfile.getString(DseDriverOption.GRAPH_SUB_PROTOCOL))
          .thenReturn(graphProtocolForTestConfig.toInternalCode());
    }
    when(testProfile.getBoolean(DseDriverOption.GRAPH_IS_SYSTEM_QUERY, false)).thenReturn(false);
    when(testProfile.getString(DseDriverOption.GRAPH_NAME, null)).thenReturn("mockGraph");
    when(testProfile.getString(DseDriverOption.GRAPH_READ_CONSISTENCY_LEVEL, null))
        .thenReturn("LOCAL_TWO");
    when(testProfile.getString(DseDriverOption.GRAPH_WRITE_CONSISTENCY_LEVEL, null))
        .thenReturn("LOCAL_THREE");
    when(config.getProfile("test-graph")).thenReturn(testProfile);

    when(systemQueryExecutionProfile.getName()).thenReturn("graph-system-query");
    when(systemQueryExecutionProfile.getDuration(DseDriverOption.GRAPH_TIMEOUT, Duration.ZERO))
        .thenReturn(Duration.ZERO);
    when(systemQueryExecutionProfile.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .thenReturn(DefaultConsistencyLevel.LOCAL_ONE.name());
    when(systemQueryExecutionProfile.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE))
        .thenReturn(5000);
    when(systemQueryExecutionProfile.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
        .thenReturn(DefaultConsistencyLevel.SERIAL.name());
    when(systemQueryExecutionProfile.getBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE))
        .thenReturn(false);
    when(systemQueryExecutionProfile.getBoolean(DefaultDriverOption.PREPARE_ON_ALL_NODES))
        .thenReturn(true);
    when(systemQueryExecutionProfile.getName()).thenReturn("graph-system-query");
    when(systemQueryExecutionProfile.getDuration(DseDriverOption.GRAPH_TIMEOUT, Duration.ZERO))
        .thenReturn(Duration.ofMillis(2));
    when(systemQueryExecutionProfile.getBoolean(DseDriverOption.GRAPH_IS_SYSTEM_QUERY, false))
        .thenReturn(true);
    when(systemQueryExecutionProfile.getString(DseDriverOption.GRAPH_READ_CONSISTENCY_LEVEL, null))
        .thenReturn("LOCAL_TWO");
    when(systemQueryExecutionProfile.getString(DseDriverOption.GRAPH_WRITE_CONSISTENCY_LEVEL, null))
        .thenReturn("LOCAL_THREE");

    when(config.getProfile("graph-system-query")).thenReturn(systemQueryExecutionProfile);

    // need to re-mock everything on the context because the RequestHandlerTestHarness returns a
    // InternalDriverContext and not a DseDriverContext. Couldn't figure out a way with mockito
    // to say "mock this object (this.dseDriverContext), and delegate every call to that
    // other object (this.context), except _this_ call and _this_ and so on"
    // Spy wouldn't work because the spied object has to be of the same type as the final object
    when(dseDriverContext.getConfig()).thenReturn(config);
    when(dseDriverContext.getNettyOptions()).thenReturn(nettyOptions);
    when(dseDriverContext.getLoadBalancingPolicyWrapper()).thenReturn(loadBalancingPolicyWrapper);
    when(dseDriverContext.getRetryPolicy(ArgumentMatchers.anyString())).thenReturn(retryPolicy);
    when(dseDriverContext.getSpeculativeExecutionPolicy(ArgumentMatchers.anyString()))
        .thenReturn(speculativeExecutionPolicy);
    when(dseDriverContext.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
    when(dseDriverContext.getTimestampGenerator()).thenReturn(timestampGenerator);
    when(dseDriverContext.getProtocolVersion()).thenReturn(DseProtocolVersion.DSE_V2);
    when(dseDriverContext.getProtocolVersionRegistry()).thenReturn(protocolVersionRegistry);
    when(dseDriverContext.getConsistencyLevelRegistry())
        .thenReturn(new DefaultConsistencyLevelRegistry());
    when(dseDriverContext.getWriteTypeRegistry()).thenReturn(new DefaultWriteTypeRegistry());
    when(dseDriverContext.getRequestThrottler())
        .thenReturn(new PassThroughRequestThrottler(dseDriverContext));
    when(dseDriverContext.getRequestTracker()).thenReturn(new NoopRequestTracker(dseDriverContext));
    // if DSE Version is specified for test metadata, then we need to mock that up on the context
    if (dseVersionForTestMetadata != null) {
      DseTestFixtures.mockNodesInMetadataWithVersions(
          dseDriverContext, true, dseVersionForTestMetadata);
    }
  }

  @Override
  public DefaultDriverContext getContext() {
    return dseDriverContext;
  }

  public static GraphRequestHandlerTestHarness.Builder builder() {
    return new GraphRequestHandlerTestHarness.Builder();
  }

  public static class Builder extends RequestHandlerTestHarness.Builder {

    private GraphProtocol graphProtocolForTestConfig;
    private Duration graphTimeout = Duration.ZERO;
    private Version dseVersionForTestMetadata;

    public GraphRequestHandlerTestHarness.Builder withGraphProtocolForTestConfig(
        GraphProtocol protocol) {
      this.graphProtocolForTestConfig = protocol;
      return this;
    }

    public GraphRequestHandlerTestHarness.Builder withDseVersionInMetadata(Version dseVersion) {
      this.dseVersionForTestMetadata = dseVersion;
      return this;
    }

    public GraphRequestHandlerTestHarness.Builder withGraphTimeout(Duration globalTimeout) {
      this.graphTimeout = globalTimeout;
      return this;
    }

    @Override
    public GraphRequestHandlerTestHarness.Builder withEmptyPool(Node node) {
      super.withEmptyPool(node);
      return this;
    }

    @Override
    public GraphRequestHandlerTestHarness.Builder withWriteFailure(Node node, Throwable cause) {
      super.withWriteFailure(node, cause);
      return this;
    }

    @Override
    public GraphRequestHandlerTestHarness.Builder withResponseFailure(Node node, Throwable cause) {
      super.withResponseFailure(node, cause);
      return this;
    }

    @Override
    public GraphRequestHandlerTestHarness.Builder withResponse(Node node, Frame response) {
      super.withResponse(node, response);
      return this;
    }

    @Override
    public GraphRequestHandlerTestHarness.Builder withDefaultIdempotence(
        boolean defaultIdempotence) {
      super.withDefaultIdempotence(defaultIdempotence);
      return this;
    }

    @Override
    public GraphRequestHandlerTestHarness.Builder withProtocolVersion(
        ProtocolVersion protocolVersion) {
      super.withProtocolVersion(protocolVersion);
      return this;
    }

    @Override
    public GraphRequestHandlerTestHarness build() {
      return new GraphRequestHandlerTestHarness(
          this, graphProtocolForTestConfig, graphTimeout, dseVersionForTestMetadata);
    }
  }
}
