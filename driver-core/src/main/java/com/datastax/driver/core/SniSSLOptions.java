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
import java.util.concurrent.CopyOnWriteArrayList;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

@IgnoreJDK6Requirement
@SuppressWarnings("deprecation")
public class SniSSLOptions extends JdkSSLOptions implements ExtendedRemoteEndpointAwareSslOptions {

  // An offset that gets added to our "fake" ports (see below). We pick this value because it is the
  // start of the ephemeral port range.
  private static final int FAKE_PORT_OFFSET = 49152;

  private final CopyOnWriteArrayList<String> fakePorts = new CopyOnWriteArrayList<String>();

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
    InetSocketAddress address = sniEndPoint.resolve();
    String sniServerName = sniEndPoint.getServerName();

    // When hostname verification is enabled (with setEndpointIdentificationAlgorithm), the SSL
    // engine will try to match the server's certificate against the SNI host name; if that doesn't
    // work, it will fall back to the "advisory peer host" passed to createSSLEngine.
    //
    // In our case, the first check will never succeed because our SNI host name is not the DNS name
    // (we use the Cassandra host_id instead). So we *must* set the advisory peer information.
    //
    // However if we use the address as-is, this leads to another issue: the advisory peer
    // information is also used to cache SSL sessions internally. All of our nodes share the same
    // proxy address, so the JDK tries to reuse SSL sessions across nodes. But it doesn't update the
    // SNI host name every time, so it ends up opening connections to the wrong node.
    //
    // To avoid that, we create a unique "fake" port for every node. We still get session reuse for
    // a given node, but not across nodes. This is safe because the advisory port is only used for
    // session caching.
    SSLEngine engine = context.createSSLEngine(address.getHostName(), getFakePort(sniServerName));
    engine.setUseClientMode(true);
    SSLParameters parameters = engine.getSSLParameters();
    parameters.setServerNames(ImmutableList.<SNIServerName>of(new SNIHostName(sniServerName)));
    parameters.setEndpointIdentificationAlgorithm("HTTPS");
    engine.setSSLParameters(parameters);
    if (cipherSuites != null) engine.setEnabledCipherSuites(cipherSuites);
    return engine;
  }

  private int getFakePort(String sniServerName) {
    fakePorts.addIfAbsent(sniServerName);
    return FAKE_PORT_OFFSET + fakePorts.indexOf(sniServerName);
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
