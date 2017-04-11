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
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy.ReconnectionSchedule;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import io.netty.channel.local.LocalAddress;
import io.netty.util.concurrent.Future;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.times;

public class ChannelPoolTest {
  private static final SocketAddress ADDRESS = new LocalAddress("test");

  private @Mock InternalDriverContext context;
  private @Mock ReconnectionPolicy reconnectionPolicy;
  private @Mock ReconnectionSchedule reconnectionSchedule;
  private @Mock NettyOptions nettyOptions;
  private @Mock ChannelFactory channelFactory;
  private BlockingQueue<CompletableFuture<DriverChannel>> channelFactoryFutures;
  private DefaultEventLoopGroup adminEventLoopGroup;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);

    adminEventLoopGroup = new DefaultEventLoopGroup(1);

    Mockito.when(context.nettyOptions()).thenReturn(nettyOptions);
    Mockito.when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventLoopGroup);
    Mockito.when(context.channelFactory()).thenReturn(channelFactory);

    channelFactoryFutures = new ArrayBlockingQueue<>(10);
    Mockito.when(channelFactory.connect(eq(ADDRESS), isNull(), anyBoolean()))
        .thenAnswer(
            invocation -> {
              CompletableFuture<DriverChannel> channelFuture = new CompletableFuture<>();
              channelFactoryFutures.offer(channelFuture);
              return channelFuture;
            });

    Mockito.when(context.reconnectionPolicy()).thenReturn(reconnectionPolicy);
    Mockito.when(reconnectionPolicy.newSchedule()).thenReturn(reconnectionSchedule);
    // By default, set a large reconnection delay. Tests that care about reconnection will override
    // it.
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofDays(1));
  }

  @AfterMethod
  public void teardown() {
    adminEventLoopGroup.shutdownGracefully(100, 200, TimeUnit.MILLISECONDS);
  }

  @Test
  public void should_initialize_when_all_channels_succeed() throws Exception {
    int channelCount = 3;
    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(ADDRESS, null, channelCount, context);

    // Pool init should have called the channel factory. Complete the responses:
    DriverChannel channel1 = newMockDriverChannel(1);
    channelFactoryFutures.take().complete(channel1);
    DriverChannel channel2 = newMockDriverChannel(2);
    channelFactoryFutures.take().complete(channel2);
    DriverChannel channel3 = newMockDriverChannel(3);
    channelFactoryFutures.take().complete(channel3);

    waitForPendingAdminTasks();

    assertThat(poolFuture)
        .isSuccess(pool -> assertThat(pool.channels).containsOnly(channel1, channel2, channel3));
  }

  @Test
  public void should_initialize_when_all_channels_fail() throws Exception {
    int channelCount = 3;
    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(ADDRESS, null, channelCount, context);

    for (int i = 0; i < channelCount; i++) {
      channelFactoryFutures
          .take()
          .completeExceptionally(new Exception("mock channel init failure"));
    }

    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess(pool -> assertThat(pool.channels).isEmpty());
  }

  @Test
  public void should_reconnect_when_init_incomplete() throws Exception {
    // Short delay so we don't have to wait in the test
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    int channelCount = 2;
    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(ADDRESS, null, channelCount, context);

    // 1 channel fails, the other succeeds
    channelFactoryFutures.take().completeExceptionally(new Exception("mock channel init failure"));
    DriverChannel channel1 = newMockDriverChannel(1);
    channelFactoryFutures.take().complete(channel1);

    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1);

    // A reconnection should have been scheduled
    Mockito.verify(reconnectionSchedule).nextDelay();

    DriverChannel channel2 = newMockDriverChannel(2);
    channelFactoryFutures.take().complete(channel2);

    waitForPendingAdminTasks();

    assertThat(pool.channels).containsOnly(channel1, channel2);
  }

  @Test
  public void should_reconnect_when_channel_dies() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    int channelCount = 2;
    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(ADDRESS, null, channelCount, context);

    // The 2 channels succeed
    DriverChannel channel1 = newMockDriverChannel(1);
    channelFactoryFutures.take().complete(channel1);
    DriverChannel channel2 = newMockDriverChannel(2);
    channelFactoryFutures.take().complete(channel2);

    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    // Simulate fatal error on channel2
    ((ChannelPromise) channel2.closeFuture())
        .setFailure(new Exception("mock channel init failure"));

    waitForPendingAdminTasks();

    Mockito.verify(reconnectionSchedule).nextDelay();

    DriverChannel channel3 = newMockDriverChannel(3);
    channelFactoryFutures.take().complete(channel3);

    waitForPendingAdminTasks();

    assertThat(pool.channels).containsOnly(channel1, channel3);
  }

  @Test
  public void should_shrink_outside_of_reconnection() throws Exception {
    int channelCount = 4;
    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(ADDRESS, null, channelCount, context);

    DriverChannel channel1 = newMockDriverChannel(1);
    channelFactoryFutures.take().complete(channel1);
    DriverChannel channel2 = newMockDriverChannel(2);
    channelFactoryFutures.take().complete(channel2);
    DriverChannel channel3 = newMockDriverChannel(3);
    channelFactoryFutures.take().complete(channel3);
    DriverChannel channel4 = newMockDriverChannel(4);
    channelFactoryFutures.take().complete(channel4);

    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4);

    pool.resize(2);

    waitForPendingAdminTasks();

    assertThat(pool.channels).containsOnly(channel3, channel4);
  }

  @Test
  public void should_shrink_during_reconnection() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    int channelCount = 4;
    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(ADDRESS, null, channelCount, context);

    DriverChannel channel1 = newMockDriverChannel(1);
    channelFactoryFutures.take().complete(channel1);
    DriverChannel channel2 = newMockDriverChannel(2);
    channelFactoryFutures.take().complete(channel2);
    channelFactoryFutures.take().completeExceptionally(new Exception("mock channel init failure"));
    channelFactoryFutures.take().completeExceptionally(new Exception("mock channel init failure"));

    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    // A reconnection should have been scheduled to add the missing channels, don't complete yet
    Mockito.verify(reconnectionSchedule).nextDelay();

    pool.resize(2);

    waitForPendingAdminTasks();

    // Now allow the reconnected channel to complete initialization
    DriverChannel channel3 = newMockDriverChannel(3);
    channelFactoryFutures.take().complete(channel3);
    DriverChannel channel4 = newMockDriverChannel(4);
    channelFactoryFutures.take().complete(channel4);

    waitForPendingAdminTasks();

    assertThat(pool.channels).containsOnly(channel3, channel4);
  }

  @Test
  public void should_grow_outside_of_reconnection() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    int channelCount = 2;
    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(ADDRESS, null, channelCount, context);

    DriverChannel channel1 = newMockDriverChannel(1);
    channelFactoryFutures.take().complete(channel1);
    DriverChannel channel2 = newMockDriverChannel(2);
    channelFactoryFutures.take().complete(channel2);

    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    pool.resize(4);

    waitForPendingAdminTasks();

    // The resizing should have triggered a reconnection
    Mockito.verify(reconnectionSchedule).nextDelay();

    DriverChannel channel3 = newMockDriverChannel(3);
    channelFactoryFutures.take().complete(channel3);
    DriverChannel channel4 = newMockDriverChannel(4);
    channelFactoryFutures.take().complete(channel4);

    waitForPendingAdminTasks();

    assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4);
  }

  @Test
  public void should_grow_during_reconnection() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    int channelCount = 2;
    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(ADDRESS, null, channelCount, context);

    DriverChannel channel1 = newMockDriverChannel(1);
    channelFactoryFutures.take().complete(channel1);
    channelFactoryFutures.take().completeExceptionally(new Exception("mock channel init failure"));

    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1);

    // A reconnection should have been scheduled to add the missing channel, don't complete yet
    Mockito.verify(reconnectionSchedule).nextDelay();

    pool.resize(4);

    waitForPendingAdminTasks();

    // Complete the channel for the first reconnection, bringing the count to 2
    DriverChannel channel2 = newMockDriverChannel(2);
    channelFactoryFutures.take().complete(channel2);

    waitForPendingAdminTasks();

    assertThat(pool.channels).containsOnly(channel1, channel2);

    // Another reconnection should have been scheduled when evaluating the first attempt
    Mockito.verify(reconnectionSchedule, times(2)).nextDelay();

    DriverChannel channel3 = newMockDriverChannel(3);
    channelFactoryFutures.take().complete(channel3);
    DriverChannel channel4 = newMockDriverChannel(4);
    channelFactoryFutures.take().complete(channel4);

    waitForPendingAdminTasks();

    assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4);
  }

  @Test
  public void should_switch_keyspace_on_existing_channels() throws Exception {
    int channelCount = 2;
    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(ADDRESS, null, channelCount, context);

    // The 2 channels succeed
    DriverChannel channel1 = newMockDriverChannel(1);
    channelFactoryFutures.take().complete(channel1);
    DriverChannel channel2 = newMockDriverChannel(2);
    channelFactoryFutures.take().complete(channel2);

    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    CqlIdentifier newKeyspace = CqlIdentifier.fromCql("new_keyspace");
    CompletionStage<Void> setKeyspaceFuture = pool.setKeyspace(newKeyspace);
    waitForPendingAdminTasks();

    Mockito.verify(channel1).setKeyspace(newKeyspace);
    Mockito.verify(channel2).setKeyspace(newKeyspace);

    assertThat(setKeyspaceFuture).isSuccess();
  }

  @Test
  public void should_switch_keyspace_on_pending_channels() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    int channelCount = 2;
    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(ADDRESS, null, channelCount, context);

    channelFactoryFutures.take().completeExceptionally(new Exception("mock channel init failure"));
    channelFactoryFutures.take().completeExceptionally(new Exception("mock channel init failure"));

    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();

    // Check that reconnection has kicked in, but do not complete it yet
    Mockito.verify(reconnectionSchedule).nextDelay();

    // Switch keyspace, it succeeds immediately since there is no active channel
    CqlIdentifier newKeyspace = CqlIdentifier.fromCql("new_keyspace");
    CompletionStage<Void> setKeyspaceFuture = pool.setKeyspace(newKeyspace);
    waitForPendingAdminTasks();
    assertThat(setKeyspaceFuture).isSuccess();

    // Now let the two channels succeed to complete the reconnection
    DriverChannel channel1 = newMockDriverChannel(1);
    channelFactoryFutures.take().complete(channel1);
    DriverChannel channel2 = newMockDriverChannel(2);
    channelFactoryFutures.take().complete(channel2);

    waitForPendingAdminTasks();

    Mockito.verify(channel1).setKeyspace(newKeyspace);
    Mockito.verify(channel2).setKeyspace(newKeyspace);
  }

  @Test
  public void should_close_all_channels_when_closed() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    int channelCount = 3;
    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(ADDRESS, null, channelCount, context);

    DriverChannel channel1 = newMockDriverChannel(1);
    channelFactoryFutures.take().complete(channel1);
    DriverChannel channel2 = newMockDriverChannel(2);
    channelFactoryFutures.take().complete(channel2);
    channelFactoryFutures.take().completeExceptionally(new Exception("mock channel init failure"));

    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();

    Mockito.verify(reconnectionSchedule).nextDelay();

    CompletionStage<ChannelPool> closeFuture = pool.close();

    // Complete the reconnection after the pool was closed
    DriverChannel channel3 = newMockDriverChannel(3);
    channelFactoryFutures.take().complete(channel3);

    waitForPendingAdminTasks();

    // The two original channels were closed normally
    Mockito.verify(channel1).close();
    Mockito.verify(channel2).close();
    // The reconnection channel was force-closed when it succeeded and we found out the pool was
    // closed
    Mockito.verify(channel3).forceClose();

    // Note that we don't wait for reconnected channels to close, so the pool only depends on
    // channel 1 and 2
    ((ChannelPromise) channel1.closeFuture()).setSuccess();
    ((ChannelPromise) channel2.closeFuture()).setSuccess();

    assertThat(closeFuture).isSuccess();
  }

  @Test
  public void should_force_close_all_channels_when_force_closed() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    int channelCount = 3;
    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(ADDRESS, null, channelCount, context);

    DriverChannel channel1 = newMockDriverChannel(1);
    channelFactoryFutures.take().complete(channel1);
    DriverChannel channel2 = newMockDriverChannel(2);
    channelFactoryFutures.take().complete(channel2);
    channelFactoryFutures.take().completeExceptionally(new Exception("mock channel init failure"));

    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();

    Mockito.verify(reconnectionSchedule).nextDelay();

    CompletionStage<ChannelPool> closeFuture = pool.forceClose();

    // Complete the reconnection after the pool was closed
    DriverChannel channel3 = newMockDriverChannel(3);
    channelFactoryFutures.take().complete(channel3);

    waitForPendingAdminTasks();

    // The two original channels were force-closed
    Mockito.verify(channel1).forceClose();
    Mockito.verify(channel2).forceClose();
    // The reconnection channel was force-closed when it succeeded and we found out the pool was
    // closed
    Mockito.verify(channel3).forceClose();

    // Note that we don't wait for reconnected channels to close, so the pool only depends on
    // channel 1 and 2
    ((ChannelPromise) channel1.closeFuture()).setSuccess();
    ((ChannelPromise) channel2.closeFuture()).setSuccess();

    assertThat(closeFuture).isSuccess();
  }

  private DriverChannel newMockDriverChannel(int id) {
    DriverChannel channel = Mockito.mock(DriverChannel.class);
    EventLoop adminExecutor = adminEventLoopGroup.next();
    DefaultChannelPromise closeFuture = new DefaultChannelPromise(null, adminExecutor);
    Mockito.when(channel.close()).thenReturn(closeFuture);
    Mockito.when(channel.forceClose()).thenReturn(closeFuture);
    Mockito.when(channel.closeFuture()).thenReturn(closeFuture);
    Mockito.when(channel.setKeyspace(any(CqlIdentifier.class)))
        .thenReturn(adminExecutor.newSucceededFuture(null));
    Mockito.when(channel.toString()).thenReturn("channel" + id);
    return channel;
  }

  // Wait for all the tasks on the pool's admin executor to complete.
  private void waitForPendingAdminTasks() {
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
