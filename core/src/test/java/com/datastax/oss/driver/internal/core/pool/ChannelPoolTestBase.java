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
package com.datastax.oss.driver.internal.core.pool;

import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.TestNodeFactory;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

abstract class ChannelPoolTestBase {

  @Mock protected InternalDriverContext context;
  @Mock private DriverConfig config;
  @Mock protected DriverExecutionProfile defaultProfile;
  @Mock private ReconnectionPolicy reconnectionPolicy;
  @Mock protected ReconnectionPolicy.ReconnectionSchedule reconnectionSchedule;
  @Mock private NettyOptions nettyOptions;
  @Mock protected ChannelFactory channelFactory;
  @Mock protected MetricsFactory metricsFactory;
  @Mock protected NodeMetricUpdater nodeMetricUpdater;
  protected DefaultNode node;
  protected EventBus eventBus;
  private DefaultEventLoopGroup adminEventLoopGroup;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    adminEventLoopGroup = new DefaultEventLoopGroup(1);

    when(context.getNettyOptions()).thenReturn(nettyOptions);
    when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventLoopGroup);
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(defaultProfile);
    this.eventBus = spy(new EventBus("test"));
    when(context.getEventBus()).thenReturn(eventBus);
    when(context.getChannelFactory()).thenReturn(channelFactory);

    when(context.getReconnectionPolicy()).thenReturn(reconnectionPolicy);
    when(reconnectionPolicy.newNodeSchedule(any(Node.class))).thenReturn(reconnectionSchedule);
    // By default, set a large reconnection delay. Tests that care about reconnection will override
    // it.
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofDays(1));

    when(context.getMetricsFactory()).thenReturn(metricsFactory);
    when(metricsFactory.newNodeUpdater(any(Node.class))).thenReturn(nodeMetricUpdater);

    node = TestNodeFactory.newNode(1, context);
  }

  @After
  public void teardown() {
    adminEventLoopGroup.shutdownGracefully(100, 200, TimeUnit.MILLISECONDS);
  }

  DriverChannel newMockDriverChannel(int id) {
    DriverChannel driverChannel = mock(DriverChannel.class);
    EventLoop adminExecutor = adminEventLoopGroup.next();
    Channel channel = mock(Channel.class);
    DefaultChannelPromise closeFuture = new DefaultChannelPromise(channel, adminExecutor);
    DefaultChannelPromise closeStartedFuture = new DefaultChannelPromise(channel, adminExecutor);
    when(driverChannel.close()).thenReturn(closeFuture);
    when(driverChannel.forceClose()).thenReturn(closeFuture);
    when(driverChannel.closeFuture()).thenReturn(closeFuture);
    when(driverChannel.closeStartedFuture()).thenReturn(closeStartedFuture);
    when(driverChannel.setKeyspace(any(CqlIdentifier.class)))
        .thenReturn(adminExecutor.newSucceededFuture(null));
    when(driverChannel.toString()).thenReturn("channel" + id);
    return driverChannel;
  }

  // Wait for all the tasks on the pool's admin executor to complete.
  void waitForPendingAdminTasks() {
    // This works because the event loop group is single-threaded
    Future<?> f = adminEventLoopGroup.schedule(() -> null, 5, TimeUnit.NANOSECONDS);
    try {
      Uninterruptibles.getUninterruptibly(f, 100, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      fail("unexpected error", e.getCause());
    } catch (TimeoutException e) {
      fail("timed out while waiting for admin tasks to complete", e);
    }
  }
}
