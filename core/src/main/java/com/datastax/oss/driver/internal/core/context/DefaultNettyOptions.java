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
package com.datastax.oss.driver.internal.core.context;

import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public class DefaultNettyOptions implements NettyOptions {
  private final EventLoopGroup ioEventLoopGroup;
  private final EventLoopGroup adminEventLoopGroup;

  public DefaultNettyOptions(String clusterName) {
    ThreadFactory safeFactory = new BlockingOperation.SafeThreadFactory();
    ThreadFactory ioThreadFactory =
        new ThreadFactoryBuilder()
            .setThreadFactory(safeFactory)
            .setNameFormat(clusterName + "-io-%d")
            .build();
    this.ioEventLoopGroup = new NioEventLoopGroup(0, ioThreadFactory);

    ThreadFactory adminThreadFactory =
        new ThreadFactoryBuilder()
            .setThreadFactory(safeFactory)
            .setNameFormat(clusterName + "-admin-%d")
            .build();
    int adminThreadCount = Math.min(2, Runtime.getRuntime().availableProcessors());
    this.adminEventLoopGroup = new DefaultEventLoopGroup(adminThreadCount, adminThreadFactory);
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
  public Future<?> onClose() {
    return ioEventLoopGroup.shutdownGracefully();
  }
}
