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
package com.datastax.oss.driver.internal.core.pool;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;

abstract class ChannelPoolTestBase {

  static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9042);
  static final Node NODE = new DefaultNode(ADDRESS);

  @Mock InternalDriverContext context;
  @Mock private DriverConfig config;
  @Mock DriverConfigProfile defaultProfile;
  @Mock private ReconnectionPolicy reconnectionPolicy;
  @Mock ReconnectionPolicy.ReconnectionSchedule reconnectionSchedule;
  @Mock private NettyOptions nettyOptions;
  @Mock ChannelFactory channelFactory;
  EventBus eventBus;
  private DefaultEventLoopGroup adminEventLoopGroup;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    adminEventLoopGroup = new DefaultEventLoopGroup(1);

    Mockito.when(context.nettyOptions()).thenReturn(nettyOptions);
    Mockito.when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventLoopGroup);
    Mockito.when(context.config()).thenReturn(config);
    Mockito.when(config.getDefaultProfile()).thenReturn(defaultProfile);
    this.eventBus = Mockito.spy(new EventBus("test"));
    Mockito.when(context.eventBus()).thenReturn(eventBus);
    Mockito.when(context.channelFactory()).thenReturn(channelFactory);

    Mockito.when(context.reconnectionPolicy()).thenReturn(reconnectionPolicy);
    Mockito.when(reconnectionPolicy.newSchedule()).thenReturn(reconnectionSchedule);
    // By default, set a large reconnection delay. Tests that care about reconnection will override
    // it.
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofDays(1));
  }

  @After
  public void teardown() {
    adminEventLoopGroup.shutdownGracefully(100, 200, TimeUnit.MILLISECONDS);
  }

  DriverChannel newMockDriverChannel(int id) {
    DriverChannel channel = Mockito.mock(DriverChannel.class);
    EventLoop adminExecutor = adminEventLoopGroup.next();
    DefaultChannelPromise closeFuture = new DefaultChannelPromise(null, adminExecutor);
    DefaultChannelPromise closeStartedFuture = new DefaultChannelPromise(null, adminExecutor);
    Mockito.when(channel.close()).thenReturn(closeFuture);
    Mockito.when(channel.forceClose()).thenReturn(closeFuture);
    Mockito.when(channel.closeFuture()).thenReturn(closeFuture);
    Mockito.when(channel.closeStartedFuture()).thenReturn(closeStartedFuture);
    Mockito.when(channel.setKeyspace(any(CqlIdentifier.class)))
        .thenReturn(adminExecutor.newSucceededFuture(null));
    Mockito.when(channel.toString()).thenReturn("channel" + id);
    return channel;
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
