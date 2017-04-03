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
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.internal.core.channel.DefaultWriteCoalescer;
import com.datastax.oss.driver.internal.core.channel.WriteCoalescer;
import com.datastax.oss.driver.internal.core.config.typesafe.TypeSafeDriverConfig;
import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.driver.internal.core.util.Reflection;
import com.datastax.oss.driver.internal.core.util.concurrent.LazyReference;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.ConfigFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.concurrent.ThreadFactory;

/**
 * Default implementation of the driver context.
 *
 * <p>All non-constant components are stored as lazy references. Deadlocks or stack overflows may
 * occur if there are cycles in the object graph.
 */
public class DefaultDriverContext implements DriverContext {

  private final LazyReference<DriverConfig> config =
      new LazyReference<>("config", this::buildDriverConfig);
  private final LazyReference<Compressor<ByteBuf>> compressor =
      new LazyReference<>("compressor", this::buildCompressor);
  private final LazyReference<FrameCodec<ByteBuf>> frameCodec =
      new LazyReference<>("frameCodec", this::buildFrameCodec);
  private final LazyReference<ProtocolVersionRegistry> protocolVersionRegistry =
      new LazyReference<>("protocolVersionRegistry", this::buildProtocolVersionRegistry);
  private final LazyReference<ThreadFactory> ioThreadFactory =
      new LazyReference<>("ioThreadFactory", this::buildIoThreadFactory);
  private final LazyReference<EventLoopGroup> ioEventLoopGroup =
      new LazyReference<>("ioEventLoopGroup", this::buildIoEventLoopGroup);
  private final LazyReference<AuthProvider> authProvider =
      new LazyReference<>("authProvider", this::buildAuthProvider);
  private final LazyReference<WriteCoalescer> writeCoalescer =
      new LazyReference<>("writeCoalescer", this::buildWriteCoalescer);

  private DriverConfig buildDriverConfig() {
    return new TypeSafeDriverConfig(
        ConfigFactory.load().getConfig("datastax-java-driver"), CoreDriverOption.values());
  }

  private Compressor<ByteBuf> buildCompressor() {
    // TODO build alternate implementation if specified in conf
    return Compressor.none();
  }

  protected FrameCodec<ByteBuf> buildFrameCodec() {
    return FrameCodec.defaultClient(
        new ByteBufPrimitiveCodec(ByteBufAllocator.DEFAULT), compressor());
  }

  protected ProtocolVersionRegistry buildProtocolVersionRegistry() {
    return new ProtocolVersionRegistry();
  }

  protected ThreadFactory buildIoThreadFactory() {
    // TODO use the driver instance's name
    return new ThreadFactoryBuilder().build();
  }

  protected EventLoopGroup buildIoEventLoopGroup() {
    return new NioEventLoopGroup(0, ioThreadFactory());
  }

  protected AuthProvider buildAuthProvider() {
    DriverConfigProfile defaultConfig = config().defaultProfile();
    CoreDriverOption classOption = CoreDriverOption.AUTHENTICATION_PROVIDER_CLASS;
    if (defaultConfig.isDefined(classOption)) {
      String className = defaultConfig.getString(classOption);
      return Reflection.buildWithConfig(
          className, AuthProvider.class, defaultConfig, classOption.getPath());
    } else {
      return AuthProvider.NONE;
    }
  }

  private WriteCoalescer buildWriteCoalescer() {
    return new DefaultWriteCoalescer(5);
  }

  @Override
  public DriverConfig config() {
    return config.get();
  }

  @Override
  public Compressor<ByteBuf> compressor() {
    return compressor.get();
  }

  @Override
  public FrameCodec<ByteBuf> frameCodec() {
    return frameCodec.get();
  }

  @Override
  public ProtocolVersionRegistry protocolVersionRegistry() {
    return protocolVersionRegistry.get();
  }

  @Override
  public ThreadFactory ioThreadFactory() {
    return ioThreadFactory.get();
  }

  @Override
  public EventLoopGroup ioEventLoopGroup() {
    return ioEventLoopGroup.get();
  }

  @Override
  public Class<? extends Channel> channelClass() {
    return NioSocketChannel.class;
  }

  @Override
  public AuthProvider authProvider() {
    return authProvider.get();
  }

  @Override
  public WriteCoalescer writeCoalescer() {
    return writeCoalescer.get();
  }
}
