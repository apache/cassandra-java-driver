/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.ssl;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;

/**
 * PEM file based SSL implementation.
 *
 * <p>To activate this class, add an {@code advanced.ssl-engine-factory} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.ssl-engine-factory {
 *     class = PEMBasedSslEngineFactory
 *     cipher-suites = [ "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" ]
 *     hostname-validation = false
 *     truststore-path = /path/to/ca.crt
 *     keystore-path = /path/to/client.key
 *     keystore-password = password123
 *   }
 * }
 * </pre>
 *
 * You can configure as follows as well:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.ssl-engine-factory {
 *     class = PEMBasedSslEngineFactory
 *     cipher-suites = [ "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" ]
 *     hostname-validation = false
 *     truststore-path = /path/to/ca.crt
 *     private-key-path = /path/to/client.key
 *     client-cert-path = /path/to/client.crt
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
public class PEMBasedSslEngineFactory implements SslEngineFactory {

  private final SslContext sslContext;
  private final String[] cipherSuites;
  private final boolean requireHostnameValidation;

  public PEMBasedSslEngineFactory(DriverContext context) {
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    try {
      this.sslContext = buildContext(config);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot initialize SSL Context", e);
    }
    if (config.isDefined(DefaultDriverOption.SSL_CIPHER_SUITES)) {
      List<String> list = config.getStringList(DefaultDriverOption.SSL_CIPHER_SUITES);
      String[] tmp = new String[list.size()];
      this.cipherSuites = list.toArray(tmp);
    } else {
      this.cipherSuites = null;
    }
    this.requireHostnameValidation =
        config.getBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, true);
  }

  private SslContext buildContext(DriverExecutionProfile config) throws SSLException {
    SslContextBuilder builder = SslContextBuilder.forClient();
    // Set either the keystore that contains both private key and client certificate, or the private
    // key and the client certificate separately
    // If set both, the error is thrown
    if (config.isDefined(DefaultDriverOption.SSL_KEYSTORE_PATH)
        && config.isDefined(DefaultDriverOption.SSL_PRIVATE_KEY_PATH)) {
      throw new IllegalStateException(
          "Cannot set both keystore-path and private-key-path at the same time");
    }
    if (config.isDefined(DefaultDriverOption.SSL_TRUSTSTORE_PATH)) {
      Path certPath = Paths.get(config.getString(DefaultDriverOption.SSL_TRUSTSTORE_PATH));
      builder.trustManager(certPath.toFile());
    }
    if (config.isDefined(DefaultDriverOption.SSL_KEYSTORE_PATH)) {
      Path keyPath = Paths.get(config.getString(DefaultDriverOption.SSL_KEYSTORE_PATH));
      String keyPassword = config.getString(DefaultDriverOption.SSL_KEYSTORE_PASSWORD, null);
      builder.keyManager(keyPath.toFile(), keyPath.toFile(), keyPassword);
    } else if (config.isDefined(DefaultDriverOption.SSL_PRIVATE_KEY_PATH)
        && config.isDefined(DefaultDriverOption.SSL_CLIENT_CERT_PATH)) {
      Path keyPath = Paths.get(config.getString(DefaultDriverOption.SSL_PRIVATE_KEY_PATH));
      Path certPath = Paths.get(config.getString(DefaultDriverOption.SSL_CLIENT_CERT_PATH));
      String keyPassword = config.getString(DefaultDriverOption.SSL_KEYSTORE_PASSWORD, null);
      builder.keyManager(certPath.toFile(), keyPath.toFile(), keyPassword);
    }
    return builder.build();
  }

  @NonNull
  @Override
  public SSLEngine newSslEngine(@NonNull EndPoint remoteEndpoint) {
    SSLEngine engine;
    SocketAddress remoteAddress = remoteEndpoint.resolve();
    if (remoteAddress instanceof InetSocketAddress) {
      InetSocketAddress socketAddress = (InetSocketAddress) remoteAddress;
      engine =
          sslContext.newEngine(
              ByteBufAllocator.DEFAULT, socketAddress.getHostName(), socketAddress.getPort());
    } else {
      engine = sslContext.newEngine(ByteBufAllocator.DEFAULT);
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

  public void close() throws Exception {
    // nothing to do
  }
}
