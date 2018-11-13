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

import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.Timer;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;

/** Low-level hooks to control certain aspects of Netty usage in the driver. */
public interface NettyOptions {

  /**
   * The event loop group that will be used for I/O. This must always return the same instance.
   *
   * <p>It is highly recommended that the threads in this event loop group be created by a {@link
   * BlockingOperation.SafeThreadFactory}, so that the driver can protect against deadlocks
   * introduced by bad client code.
   */
  EventLoopGroup ioEventLoopGroup();

  /**
   * The class to create {@code Channel} instances from. This must be consistent with {@link
   * #ioEventLoopGroup()}.
   */
  Class<? extends Channel> channelClass();

  /**
   * An event executor group that will be used to schedule all tasks not related to request I/O:
   * cluster events, refreshing metadata, reconnection, etc.
   *
   * <p>This must always return the same instance (it can be the same object as {@link
   * #ioEventLoopGroup()}).
   *
   * <p>It is highly recommended that the threads in this event loop group be created by a {@link
   * BlockingOperation.SafeThreadFactory}, so that the driver can protect against deadlocks
   * introduced by bad client code.
   */
  EventExecutorGroup adminEventExecutorGroup();

  /**
   * The byte buffer allocator to use. This must always return the same instance. Note that this is
   * also used by the default implementation of {@link InternalDriverContext#getFrameCodec()}, and
   * the built-in {@link com.datastax.oss.protocol.internal.Compressor} implementations.
   */
  ByteBufAllocator allocator();

  /**
   * A hook invoked each time the driver creates a client bootstrap in order to open a channel. This
   * is a good place to configure any custom option on the bootstrap.
   */
  void afterBootstrapInitialized(Bootstrap bootstrap);

  /**
   * A hook invoked on each channel, right after the channel has initialized it. This is a good
   * place to register any custom handler on the channel's pipeline (note that built-in driver
   * handlers are already installed at that point).
   */
  void afterChannelInitialized(Channel channel);

  /**
   * A hook involved when the driver instance shuts down. This is a good place to free any resources
   * that you have allocated elsewhere in this component, for example shut down custom event loop
   * groups.
   */
  Future<Void> onClose();

  /**
   * The Timer on which non-I/O events should be scheduled. This must always return the same
   * instance. This timer should be used for things like request timeout events and scheduling
   * speculative executions. Under high load, scheduling these non-I/O events on a separate, lower
   * resolution timer will allow for higher overall I/O throughput.
   */
  Timer getTimer();
}
