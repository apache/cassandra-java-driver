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
package com.datastax.oss.driver.internal.core.control;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.addresstranslation.PassThroughAddressTranslator;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.DriverChannelOptions;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.LoadBalancingPolicyWrapper;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.MockUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;

abstract class ControlConnectionTestBase {
  protected static final InetSocketAddress ADDRESS1 = new InetSocketAddress("127.0.0.1", 9042);
  protected static final InetSocketAddress ADDRESS2 = new InetSocketAddress("127.0.0.2", 9042);
  protected static final DefaultNode NODE1 = new DefaultNode(ADDRESS1);
  protected static final DefaultNode NODE2 = new DefaultNode(ADDRESS2);

  @Mock protected InternalDriverContext context;
  @Mock protected ReconnectionPolicy reconnectionPolicy;
  @Mock protected ReconnectionPolicy.ReconnectionSchedule reconnectionSchedule;
  @Mock protected NettyOptions nettyOptions;
  protected DefaultEventLoopGroup adminEventLoopGroup;
  @Mock protected EventBus eventBus;
  @Mock protected ChannelFactory channelFactory;
  protected Exchanger<CompletableFuture<DriverChannel>> channelFactoryFuture;
  @Mock protected LoadBalancingPolicyWrapper loadBalancingPolicyWrapper;
  @Mock protected MetadataManager metadataManager;
  protected AddressTranslator addressTranslator;

  protected ControlConnection controlConnection;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);

    adminEventLoopGroup = new DefaultEventLoopGroup(1);

    Mockito.when(context.nettyOptions()).thenReturn(nettyOptions);
    Mockito.when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventLoopGroup);
    Mockito.when(context.eventBus()).thenReturn(eventBus);
    Mockito.when(context.channelFactory()).thenReturn(channelFactory);

    channelFactoryFuture = new Exchanger<>();
    Mockito.when(channelFactory.connect(any(Node.class), any(DriverChannelOptions.class)))
        .thenAnswer(
            invocation -> {
              CompletableFuture<DriverChannel> channelFuture = new CompletableFuture<>();
              channelFactoryFuture.exchange(channelFuture, 100, TimeUnit.MILLISECONDS);
              return channelFuture;
            });

    Mockito.when(context.reconnectionPolicy()).thenReturn(reconnectionPolicy);
    Mockito.when(reconnectionPolicy.newSchedule()).thenReturn(reconnectionSchedule);
    // By default, set a large reconnection delay. Tests that care about reconnection will override
    // it.
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofDays(1));

    Mockito.when(context.loadBalancingPolicyWrapper()).thenReturn(loadBalancingPolicyWrapper);
    Mockito.when(loadBalancingPolicyWrapper.newQueryPlan())
        .thenAnswer(
            i -> {
              ConcurrentLinkedQueue<Node> queryPlan = new ConcurrentLinkedQueue<>();
              queryPlan.offer(NODE1);
              queryPlan.offer(NODE2);
              return queryPlan;
            });

    Mockito.when(metadataManager.refreshNodes())
        .thenReturn(CompletableFuture.completedFuture(null));
    Mockito.when(context.metadataManager()).thenReturn(metadataManager);

    addressTranslator = Mockito.spy(new PassThroughAddressTranslator(context));
    Mockito.when(context.addressTranslator()).thenReturn(addressTranslator);

    controlConnection = new ControlConnection(context);
  }

  @AfterMethod
  public void teardown() {
    adminEventLoopGroup.shutdownGracefully(100, 200, TimeUnit.MILLISECONDS);
  }

  protected DriverChannel newMockDriverChannel(int id) {
    DriverChannel channel = Mockito.mock(DriverChannel.class);
    EventLoop adminExecutor = adminEventLoopGroup.next();
    DefaultChannelPromise closeFuture = new DefaultChannelPromise(null, adminExecutor);
    Mockito.when(channel.close())
        .thenAnswer(
            i -> {
              closeFuture.trySuccess(null);
              return closeFuture;
            });
    Mockito.when(channel.forceClose())
        .thenAnswer(
            i -> {
              closeFuture.trySuccess(null);
              return closeFuture;
            });
    Mockito.when(channel.closeFuture()).thenReturn(closeFuture);
    Mockito.when(channel.toString()).thenReturn("channel" + id);
    Mockito.when(channel.address()).thenReturn(new InetSocketAddress("127.0.0." + id, 9042));
    return channel;
  }

  protected static void failChannel(DriverChannel channel, String message) {
    assertThat(MockUtil.isMock(channel)).isTrue();
    ((DefaultChannelPromise) channel.closeFuture()).setFailure(new Exception(message));
  }

  // Wait for all the tasks on the admin executor to complete.
  protected void waitForPendingAdminTasks() {
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
