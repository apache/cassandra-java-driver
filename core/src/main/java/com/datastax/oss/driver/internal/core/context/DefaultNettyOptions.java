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
package com.datastax.oss.driver.internal.core.context;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.PromiseCombiner;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultNettyOptions implements NettyOptions {
  private final EventLoopGroup ioEventLoopGroup;
  private final EventLoopGroup adminEventLoopGroup;
  private final int ioShutdownQuietPeriod;
  private final int ioShutdownTimeout;
  private final TimeUnit ioShutdownUnit;
  private final int adminShutdownQuietPeriod;
  private final int adminShutdownTimeout;
  private final TimeUnit adminShutdownUnit;

  public DefaultNettyOptions(InternalDriverContext context) {
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    int ioGroupSize = config.getInt(DefaultDriverOption.NETTY_IO_SIZE);
    this.ioShutdownQuietPeriod = config.getInt(DefaultDriverOption.NETTY_IO_SHUTDOWN_QUIET_PERIOD);
    this.ioShutdownTimeout = config.getInt(DefaultDriverOption.NETTY_IO_SHUTDOWN_TIMEOUT);
    this.ioShutdownUnit =
        TimeUnit.valueOf(config.getString(DefaultDriverOption.NETTY_IO_SHUTDOWN_UNIT));
    int adminGroupSize = config.getInt(DefaultDriverOption.NETTY_ADMIN_SIZE);
    this.adminShutdownQuietPeriod =
        config.getInt(DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD);
    this.adminShutdownTimeout = config.getInt(DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_TIMEOUT);
    this.adminShutdownUnit =
        TimeUnit.valueOf(config.getString(DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_UNIT));

    ThreadFactory safeFactory = new BlockingOperation.SafeThreadFactory();
    ThreadFactory ioThreadFactory =
        new ThreadFactoryBuilder()
            .setThreadFactory(safeFactory)
            .setNameFormat(context.getSessionName() + "-io-%d")
            .build();
    this.ioEventLoopGroup = new NioEventLoopGroup(ioGroupSize, ioThreadFactory);

    ThreadFactory adminThreadFactory =
        new ThreadFactoryBuilder()
            .setThreadFactory(safeFactory)
            .setNameFormat(context.getSessionName() + "-admin-%d")
            .build();
    this.adminEventLoopGroup = new DefaultEventLoopGroup(adminGroupSize, adminThreadFactory);
  }

  @Override
  public EventLoopGroup ioEventLoopGroup() {
    return ioEventLoopGroup;
  }

  @Override
  public EventExecutorGroup adminEventExecutorGroup() {
    return adminEventLoopGroup;
  }

  @Override
  public Class<? extends Channel> channelClass() {
    return NioSocketChannel.class;
  }

  @Override
  public ByteBufAllocator allocator() {
    return ByteBufAllocator.DEFAULT;
  }

  @Override
  public void afterBootstrapInitialized(Bootstrap bootstrap) {
    // nothing to do
  }

  @Override
  public void afterChannelInitialized(Channel channel) {
    // nothing to do
  }

  @Override
  public Future<Void> onClose() {
    PromiseCombiner combiner = new PromiseCombiner();
    combiner.add(
        adminEventLoopGroup.shutdownGracefully(
            adminShutdownQuietPeriod, adminShutdownTimeout, adminShutdownUnit));
    combiner.add(
        ioEventLoopGroup.shutdownGracefully(
            ioShutdownQuietPeriod, ioShutdownTimeout, ioShutdownUnit));
    DefaultPromise<Void> closeFuture = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
    combiner.finish(closeFuture);
    return closeFuture;
  }
}
