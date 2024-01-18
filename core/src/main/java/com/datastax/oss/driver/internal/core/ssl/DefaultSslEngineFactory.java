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
package com.datastax.oss.driver.internal.core.ssl;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import net.jcip.annotations.ThreadSafe;

/**
 * Default SSL implementation.
 *
 * <p>To activate this class, add an {@code advanced.ssl-engine-factory} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.ssl-engine-factory {
 *     class = DefaultSslEngineFactory
 *     cipher-suites = [ "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" ]
 *     hostname-validation = false
 *     truststore-path = /path/to/client.truststore
 *     truststore-password = password123
 *     keystore-path = /path/to/client.keystore
 *     keystore-password = password123
 *     keystore-reload-interval = 30 minutes
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
@ThreadSafe
public class DefaultSslEngineFactory implements SslEngineFactory {

  private final SSLContext sslContext;
  private final String[] cipherSuites;
  private final boolean requireHostnameValidation;
  private ReloadingKeyManagerFactory kmf;

  /** Builds a new instance from the driver configuration. */
  public DefaultSslEngineFactory(DriverContext driverContext) {
    DriverExecutionProfile config = driverContext.getConfig().getDefaultProfile();
    try {
      this.sslContext = buildContext(config);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot initialize SSL Context", e);
    }
    if (config.isDefined(DefaultDriverOption.SSL_CIPHER_SUITES)) {
      List<String> list = config.getStringList(DefaultDriverOption.SSL_CIPHER_SUITES);
      String tmp[] = new String[list.size()];
      this.cipherSuites = list.toArray(tmp);
    } else {
      this.cipherSuites = null;
    }
    this.requireHostnameValidation =
        config.getBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, true);
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

  protected SSLContext buildContext(DriverExecutionProfile config) throws Exception {
    if (config.isDefined(DefaultDriverOption.SSL_KEYSTORE_PATH)
        || config.isDefined(DefaultDriverOption.SSL_TRUSTSTORE_PATH)) {
      SSLContext context = SSLContext.getInstance("SSL");

      // initialize truststore if configured.
      TrustManagerFactory tmf = null;
      if (config.isDefined(DefaultDriverOption.SSL_TRUSTSTORE_PATH)) {
        try (InputStream tsf =
            Files.newInputStream(
                Paths.get(config.getString(DefaultDriverOption.SSL_TRUSTSTORE_PATH)))) {
          KeyStore ts = KeyStore.getInstance("JKS");
          char[] password =
              config.isDefined(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD)
                  ? config.getString(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD).toCharArray()
                  : null;
          ts.load(tsf, password);
          tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
          tmf.init(ts);
        }
      }

      // initialize keystore if configured.
      if (config.isDefined(DefaultDriverOption.SSL_KEYSTORE_PATH)) {
        kmf = buildReloadingKeyManagerFactory(config);
      }

      context.init(
          kmf != null ? kmf.getKeyManagers() : null,
          tmf != null ? tmf.getTrustManagers() : null,
          new SecureRandom());
      return context;
    } else {
      // if both keystore and truststore aren't configured, use default SSLContext.
      return SSLContext.getDefault();
    }
  }

  private ReloadingKeyManagerFactory buildReloadingKeyManagerFactory(
      DriverExecutionProfile config) {
    Path keystorePath = Paths.get(config.getString(DefaultDriverOption.SSL_KEYSTORE_PATH));
    String password =
        config.isDefined(DefaultDriverOption.SSL_KEYSTORE_PASSWORD)
            ? config.getString(DefaultDriverOption.SSL_KEYSTORE_PASSWORD)
            : null;
    Duration reloadInterval =
        config.isDefined(DefaultDriverOption.SSL_KEYSTORE_RELOAD_INTERVAL)
            ? config.getDuration(DefaultDriverOption.SSL_KEYSTORE_RELOAD_INTERVAL)
            : Duration.ZERO;
    return ReloadingKeyManagerFactory.create(keystorePath, password, reloadInterval);
  }

  @Override
  public void close() throws Exception {
    kmf.close();
  }
}
