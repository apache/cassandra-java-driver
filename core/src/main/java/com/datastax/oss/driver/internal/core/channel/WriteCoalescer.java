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
package com.datastax.oss.driver.internal.core.channel;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * Optimizes the write operations on Netty channels.
 *
 * <p>Flush operations are generally speaking expensive as these may trigger a syscall on the
 * transport level. Thus it is in most cases (where write latency can be traded with throughput) a
 * good idea to try to minimize flush operations as much as possible. This component allows writes
 * to be accumulated and flushed together for better performance.
 */
public interface WriteCoalescer {
  /**
   * Writes and flushes the message to the channel, possibly at a later time, but <b>the order of
   * messages must be preserved</b>.
   */
  ChannelFuture writeAndFlush(Channel channel, Object message);
}
