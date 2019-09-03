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
package com.datastax.driver.core;

import com.google.common.collect.ImmutableList;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import java.net.InetSocketAddress;
import javax.net.ssl.*;

@IgnoreJDK6Requirement
@SuppressWarnings("deprecation")
public class SniSSLOptions extends JdkSSLOptions implements ExtendedRemoteEndpointAwareSslOptions {
  /**
   * Creates a new instance.
   *
   * @param context the SSL context.
   * @param cipherSuites the cipher suites to use.
   */
  protected SniSSLOptions(SSLContext context, String[] cipherSuites) {
    super(context, cipherSuites);
  }

  @Override
  public SslHandler newSSLHandler(SocketChannel channel) {
    throw new AssertionError(
        "This class implements RemoteEndpointAwareSSLOptions, this method should not be called");
  }

  @Override
  public SslHandler newSSLHandler(SocketChannel channel, EndPoint remoteEndpoint) {
    SSLEngine engine = newSSLEngine(channel, remoteEndpoint);
    return new SslHandler(engine);
  }

  @Override
  public SslHandler newSSLHandler(SocketChannel channel, InetSocketAddress remoteEndpoint) {
    throw new AssertionError(
        "The driver should never call this method on an object that implements "
            + this.getClass().getSimpleName());
  }

  protected SSLEngine newSSLEngine(
      @SuppressWarnings("unused") SocketChannel channel, EndPoint remoteEndpoint) {
    if (!(remoteEndpoint instanceof SniEndPoint)) {
      throw new IllegalArgumentException(
          String.format(
              "Configuration error: can only use %s with SNI end points",
              this.getClass().getSimpleName()));
    }
    SniEndPoint sniEndPoint = (SniEndPoint) remoteEndpoint;
    SSLEngine engine = context.createSSLEngine();
    engine.setUseClientMode(true);
    SSLParameters parameters = engine.getSSLParameters();
    parameters.setServerNames(
        ImmutableList.<SNIServerName>of(new SNIHostName(sniEndPoint.getServerName())));
    engine.setSSLParameters(parameters);
    if (cipherSuites != null) engine.setEnabledCipherSuites(cipherSuites);
    return engine;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends JdkSSLOptions.Builder {

    @Override
    public SniSSLOptions.Builder withSSLContext(SSLContext context) {
      super.withSSLContext(context);
      return this;
    }

    @Override
    public SniSSLOptions.Builder withCipherSuites(String[] cipherSuites) {
      super.withCipherSuites(cipherSuites);
      return this;
    }

    @Override
    public SniSSLOptions build() {
      return new SniSSLOptions(context, cipherSuites);
    }
  }
}
