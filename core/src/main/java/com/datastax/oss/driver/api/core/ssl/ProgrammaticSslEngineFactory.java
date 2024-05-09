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
package com.datastax.oss.driver.api.core.ssl;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

/**
 * An SSL engine factory that allows you to configure the driver programmatically, by passing your
 * own {@link SSLContext}.
 *
 * <p>Note that this class will create SSL engines with advisory peer information ({@link
 * SSLContext#createSSLEngine(String, int)}) whenever possible.
 *
 * <p>If those defaults do not work for you, it should be pretty straightforward to write your own
 * implementation by extending or duplicating this class.
 *
 * @see SessionBuilder#withSslEngineFactory(SslEngineFactory)
 * @see SessionBuilder#withSslContext(SSLContext)
 */
public class ProgrammaticSslEngineFactory implements SslEngineFactory {

  protected final SSLContext sslContext;
  protected final String[] cipherSuites;
  protected final boolean requireHostnameValidation;

  /**
   * Creates an instance with the given {@link SSLContext}, default cipher suites and no host name
   * validation.
   *
   * @param sslContext the {@link SSLContext} to use.
   */
  public ProgrammaticSslEngineFactory(@NonNull SSLContext sslContext) {
    this(sslContext, null);
  }

  /**
   * Creates an instance with the given {@link SSLContext} and cipher suites, and no host name
   * validation.
   *
   * @param sslContext the {@link SSLContext} to use.
   * @param cipherSuites the cipher suites to use, or null to use the default ones.
   */
  public ProgrammaticSslEngineFactory(
      @NonNull SSLContext sslContext, @Nullable String[] cipherSuites) {
    this(sslContext, cipherSuites, false);
  }

  /**
   * Creates an instance with the given {@link SSLContext}, cipher suites and host name validation.
   *
   * @param sslContext the {@link SSLContext} to use.
   * @param cipherSuites the cipher suites to use, or null to use the default ones.
   * @param requireHostnameValidation whether to enable host name validation. If enabled, host name
   *     validation will be done using HTTPS algorithm.
   */
  public ProgrammaticSslEngineFactory(
      @NonNull SSLContext sslContext,
      @Nullable String[] cipherSuites,
      boolean requireHostnameValidation) {
    this.sslContext = sslContext;
    this.cipherSuites = cipherSuites;
    this.requireHostnameValidation = requireHostnameValidation;
  }

  @NonNull
  @Override
  public SSLEngine newSslEngine(@NonNull EndPoint remoteEndpoint) {
    SSLEngine engine;
    SocketAddress remoteAddress = remoteEndpoint.resolve();
    if (remoteAddress instanceof InetSocketAddress) {
      InetSocketAddress socketAddress = (InetSocketAddress) remoteAddress;
      engine = sslContext.createSSLEngine(socketAddress.getHostName(), socketAddress.getPort());
    } else {
      engine = sslContext.createSSLEngine();
    }
    engine.setUseClientMode(true);
    if (cipherSuites != null) {
      engine.setEnabledCipherSuites(cipherSuites);
    }
    if (requireHostnameValidation) {
      SSLParameters parameters = engine.getSSLParameters();
      parameters.setEndpointIdentificationAlgorithm("HTTPS");
      engine.setSSLParameters(parameters);
    }
    return engine;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
