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

package com.datastax.oss.driver.internal.core.ssl;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.metadata.SniEndPoint;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

public class SniSslEngineFactory implements SslEngineFactory {

  private final SSLContext sslContext;

  /** Builds a new instance from the driver configuration. */
  public SniSslEngineFactory(SSLContext sslContext) {
    this.sslContext = sslContext;
  }

  @NonNull
  @Override
  public SSLEngine newSslEngine(@NonNull EndPoint remoteEndpoint) {
    if (!(remoteEndpoint instanceof SniEndPoint)) {
      throw new IllegalArgumentException(
          String.format(
              "Configuration error: can only use %s with SNI end points",
              this.getClass().getSimpleName()));
    }
    SSLEngine engine;
    SniEndPoint sniEndPoint = (SniEndPoint) remoteEndpoint;

    engine = sslContext.createSSLEngine();
    engine.setUseClientMode(true);
    SSLParameters parameters = engine.getSSLParameters();
    parameters.setServerNames(ImmutableList.of(new SNIHostName(sniEndPoint.getServerName())));
    engine.setSSLParameters(parameters);
    return engine;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
