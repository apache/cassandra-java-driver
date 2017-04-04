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
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.channel.DefaultWriteCoalescer;
import com.datastax.oss.driver.internal.core.channel.WriteCoalescer;
import com.datastax.oss.driver.internal.core.config.typesafe.TypeSafeDriverConfig;
import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.driver.internal.core.ssl.JdkSslHandlerFactory;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import com.datastax.oss.driver.internal.core.util.Reflection;
import com.datastax.oss.driver.internal.core.util.concurrent.LazyReference;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.typesafe.config.ConfigFactory;
import io.netty.buffer.ByteBuf;

/**
 * Default implementation of the driver context.
 *
 * <p>All non-constant components are stored as lazy references. Deadlocks or stack overflows may
 * occur if there are cycles in the object graph.
 */
public class DefaultDriverContext implements DriverContext {

  private final LazyReference<DriverConfig> configRef =
      new LazyReference<>("config", this::buildDriverConfig);
  private final LazyReference<Compressor<ByteBuf>> compressorRef =
      new LazyReference<>("compressor", this::buildCompressor);
  private final LazyReference<FrameCodec<ByteBuf>> frameCodecRef =
      new LazyReference<>("frameCodec", this::buildFrameCodec);
  private final LazyReference<ProtocolVersionRegistry> protocolVersionRegistryRef =
      new LazyReference<>("protocolVersionRegistry", this::buildProtocolVersionRegistry);
  private final LazyReference<NettyOptions> nettyOptionsRef =
      new LazyReference<>("nettyOptions", this::buildNettyOptions);
  private final LazyReference<AuthProvider> authProviderRef =
      new LazyReference<>("authProvider", this::buildAuthProvider);
  private final LazyReference<WriteCoalescer> writeCoalescerRef =
      new LazyReference<>("writeCoalescer", this::buildWriteCoalescer);
  private final LazyReference<SslHandlerFactory> sslHandlerFactoryRef =
      new LazyReference<>("sslHandlerFactory", this::buildSslHandlerFactory);

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
        new ByteBufPrimitiveCodec(nettyOptions().allocator()), compressor());
  }

  protected ProtocolVersionRegistry buildProtocolVersionRegistry() {
    return new ProtocolVersionRegistry();
  }

  protected NettyOptions buildNettyOptions() {
    return new DefaultNettyOptions();
  }

  protected AuthProvider buildAuthProvider() {
    AuthProvider configuredProvider =
        Reflection.buildFromConfig(
            config().defaultProfile(),
            CoreDriverOption.AUTHENTICATION_PROVIDER_CLASS,
            AuthProvider.class);
    return (configuredProvider == null) ? AuthProvider.NONE : configuredProvider;
  }

  protected SslHandlerFactory buildSslHandlerFactory() {
    // If a JDK-based factory was provided through the public API, wrap that, otherwise no SSL.
    SslEngineFactory sslEngineFactory =
        Reflection.buildFromConfig(
            config().defaultProfile(), CoreDriverOption.SSL_FACTORY_CLASS, SslEngineFactory.class);
    return (sslEngineFactory == null)
        ? SslHandlerFactory.NONE
        : new JdkSslHandlerFactory(sslEngineFactory);

    // For more advanced options (like using Netty's native OpenSSL support instead of the JDK),
    // extend DefaultDriverContext and override this method
  }

  private WriteCoalescer buildWriteCoalescer() {
    return new DefaultWriteCoalescer(5);
  }

  @Override
  public DriverConfig config() {
    return configRef.get();
  }

  @Override
  public Compressor<ByteBuf> compressor() {
    return compressorRef.get();
  }

  @Override
  public FrameCodec<ByteBuf> frameCodec() {
    return frameCodecRef.get();
  }

  @Override
  public ProtocolVersionRegistry protocolVersionRegistry() {
    return protocolVersionRegistryRef.get();
  }

  @Override
  public NettyOptions nettyOptions() {
    return nettyOptionsRef.get();
  }

  @Override
  public AuthProvider authProvider() {
    return authProviderRef.get();
  }

  @Override
  public WriteCoalescer writeCoalescer() {
    return writeCoalescerRef.get();
  }

  @Override
  public SslHandlerFactory sslHandlerFactory() {
    return sslHandlerFactoryRef.get();
  }
}
