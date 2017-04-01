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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.FrameCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import java.util.concurrent.ThreadFactory;

/**
 * Holder for common singletons that are shared throughout the driver.
 *
 * <p>There is an instance of this class for each driver instance.
 *
 * <p>This is essentially poor man's dependency injection (even the simplest DI frameworks are too
 * overkill for our needs, and they introduce extra dependencies).
 *
 * <p>This also provides extension points for stuff that is too low-level for the configuration.
 */
public interface DriverContext {

  DriverConfig config();

  Compressor<ByteBuf> compressor();

  FrameCodec<ByteBuf> frameCodec();

  ProtocolVersionRegistry protocolVersionRegistry();

  ThreadFactory ioThreadFactory();

  EventLoopGroup ioEventLoopGroup();

  Class<? extends Channel> channelClass();

  AuthProvider authProvider();
}
