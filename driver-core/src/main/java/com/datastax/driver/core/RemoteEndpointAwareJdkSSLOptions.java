/*
 * Copyright (C) 2012-2017 DataStax Inc.
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

import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;

/**
 * {@link RemoteEndpointAwareSSLOptions} implementation based on built-in JDK classes.
 *
 * @see <a href="https://datastax-oss.atlassian.net/browse/JAVA-1364">JAVA-1364</a>
 * @since 3.2.0
 */
@SuppressWarnings("deprecation")
public class RemoteEndpointAwareJdkSSLOptions extends JdkSSLOptions implements RemoteEndpointAwareSSLOptions {

    /**
     * Creates a builder to create a new instance.
     *
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a new instance.
     *
     * @param context      the SSL context.
     * @param cipherSuites the cipher suites to use.
     */
    protected RemoteEndpointAwareJdkSSLOptions(SSLContext context, String[] cipherSuites) {
        super(context, cipherSuites);
    }

    @Override
    public SslHandler newSSLHandler(SocketChannel channel) {
        throw new AssertionError("This class implements RemoteEndpointAwareSSLOptions, this method should not be called");
    }

    @Override
    public SslHandler newSSLHandler(SocketChannel channel, InetSocketAddress remoteEndpoint) {
        SSLEngine engine = newSSLEngine(channel, remoteEndpoint);
        return new SslHandler(engine);
    }

    /**
     * Creates an SSL engine each time a connection is established.
     * <p/>
     * You might want to override this if you need to fine-tune the engine's configuration
     * (for example enabling hostname verification).
     *
     * @param channel        the Netty channel for that connection.
     * @param remoteEndpoint the remote endpoint we are connecting to.
     * @return the engine.
     * @since 3.2.0
     */
    protected SSLEngine newSSLEngine(@SuppressWarnings("unused") SocketChannel channel, InetSocketAddress remoteEndpoint) {
        SSLEngine engine = remoteEndpoint == null
                ? context.createSSLEngine()
                : context.createSSLEngine(remoteEndpoint.getHostName(), remoteEndpoint.getPort());
        engine.setUseClientMode(true);
        if (cipherSuites != null)
            engine.setEnabledCipherSuites(cipherSuites);
        return engine;
    }

    /**
     * Helper class to build JDK-based SSL options.
     */
    public static class Builder extends JdkSSLOptions.Builder {

        /**
         * Builds a new instance based on the parameters provided to this builder.
         *
         * @return the new instance.
         */
        @Override
        public RemoteEndpointAwareJdkSSLOptions build() {
            return new RemoteEndpointAwareJdkSSLOptions(context, cipherSuites);
        }
    }
}
