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
package com.datastax.oss.driver.internal.core.control;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.DriverChannelOptions;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.LoadBalancingPolicyWrapper;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.metadata.TestNodeFactory;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.verification.VerificationWithTimeout;

abstract class ControlConnectionTestBase {
  protected static final InetSocketAddress ADDRESS1 = new InetSocketAddress("127.0.0.1", 9042);

  /** How long we wait when verifying mocks for async invocations */
  protected static final VerificationWithTimeout VERIFY_TIMEOUT = timeout(500);

  @Mock protected InternalDriverContext context;
  @Mock protected DriverConfig config;
  @Mock protected DriverExecutionProfile defaultProfile;
  @Mock protected ReconnectionPolicy reconnectionPolicy;
  @Mock protected ReconnectionPolicy.ReconnectionSchedule reconnectionSchedule;
  @Mock protected NettyOptions nettyOptions;
  protected DefaultEventLoopGroup adminEventLoopGroup;
  protected EventBus eventBus;
  @Mock protected ChannelFactory channelFactory;
  protected Exchanger<CompletableFuture<DriverChannel>> channelFactoryFuture;
  @Mock protected LoadBalancingPolicyWrapper loadBalancingPolicyWrapper;
  @Mock protected MetadataManager metadataManager;
  @Mock protected MetricsFactory metricsFactory;

  protected DefaultNode node1;
  protected DefaultNode node2;

  protected ControlConnection controlConnection;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    adminEventLoopGroup = new DefaultEventLoopGroup(1);

    when(context.getNettyOptions()).thenReturn(nettyOptions);
    when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventLoopGroup);
    eventBus = spy(new EventBus("test"));
    when(context.getEventBus()).thenReturn(eventBus);
    when(context.getChannelFactory()).thenReturn(channelFactory);

    channelFactoryFuture = new Exchanger<>();
    when(channelFactory.connect(any(Node.class), any(DriverChannelOptions.class)))
        .thenAnswer(
            invocation -> {
              CompletableFuture<DriverChannel> channelFuture = new CompletableFuture<>();
              channelFactoryFuture.exchange(channelFuture, 100, TimeUnit.MILLISECONDS);
              return channelFuture;
            });

    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(defaultProfile);
    when(defaultProfile.getBoolean(DefaultDriverOption.RECONNECT_ON_INIT)).thenReturn(false);

    when(context.getReconnectionPolicy()).thenReturn(reconnectionPolicy);
    // Child classes only cover "runtime" reconnections when the driver is already initialized
    when(reconnectionPolicy.newControlConnectionSchedule(false)).thenReturn(reconnectionSchedule);
    // By default, set a large reconnection delay. Tests that care about reconnection will override
    // it.
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofDays(1));

    when(context.getLoadBalancingPolicyWrapper()).thenReturn(loadBalancingPolicyWrapper);

    when(context.getMetricsFactory()).thenReturn(metricsFactory);
    node1 = TestNodeFactory.newNode(1, context);
    node2 = TestNodeFactory.newNode(2, context);
    mockQueryPlan(node1, node2);

    when(metadataManager.refreshNodes()).thenReturn(CompletableFuture.completedFuture(null));
    when(metadataManager.refreshSchema(anyString(), anyBoolean(), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(context.getMetadataManager()).thenReturn(metadataManager);

    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(defaultProfile);
    when(defaultProfile.getBoolean(DefaultDriverOption.CONNECTION_WARN_INIT_ERROR))
        .thenReturn(false);

    controlConnection = new ControlConnection(context);
  }

  protected void mockQueryPlan(Node... nodes) {
    when(loadBalancingPolicyWrapper.newQueryPlan())
        .thenAnswer(
            i -> {
              ConcurrentLinkedQueue<Node> queryPlan = new ConcurrentLinkedQueue<>();
              for (Node node : nodes) {
                queryPlan.offer(node);
              }
              return queryPlan;
            });
  }

  @After
  public void teardown() {
    adminEventLoopGroup.shutdownGracefully(100, 200, TimeUnit.MILLISECONDS);
  }

  protected DriverChannel newMockDriverChannel(int id) {
    DriverChannel driverChannel = mock(DriverChannel.class);
    Channel channel = mock(Channel.class);
    EventLoop adminExecutor = adminEventLoopGroup.next();
    DefaultChannelPromise closeFuture = new DefaultChannelPromise(channel, adminExecutor);
    when(driverChannel.close())
        .thenAnswer(
            i -> {
              closeFuture.trySuccess(null);
              return closeFuture;
            });
    when(driverChannel.forceClose())
        .thenAnswer(
            i -> {
              closeFuture.trySuccess(null);
              return closeFuture;
            });
    when(driverChannel.closeFuture()).thenReturn(closeFuture);
    when(driverChannel.toString()).thenReturn("channel" + id);
    when(driverChannel.getEndPoint())
        .thenReturn(new DefaultEndPoint(new InetSocketAddress("127.0.0." + id, 9042)));
    return driverChannel;
  }
}
