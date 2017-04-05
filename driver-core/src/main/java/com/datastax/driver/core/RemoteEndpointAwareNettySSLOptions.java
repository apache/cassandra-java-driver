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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

import java.net.InetSocketAddress;

/**
 * {@link RemoteEndpointAwareSSLOptions} implementation based on Netty's SSL context.
 * <p/>
 * Netty has the ability to use OpenSSL if available, instead of the JDK's built-in engine. This yields better performance.
 *
 * @see <a href="https://datastax-oss.atlassian.net/browse/JAVA-1364">JAVA-1364</a>
 * @since 3.2.0
 */
@SuppressWarnings("deprecation")
public class RemoteEndpointAwareNettySSLOptions extends NettySSLOptions implements RemoteEndpointAwareSSLOptions {

    /**
     * Create a new instance from a given context.
     *
     * @param context the Netty context. {@code SslContextBuilder.forClient()} provides a fluent API to build it.
     */
    public RemoteEndpointAwareNettySSLOptions(SslContext context) {
        super(context);
    }

    @Override
    public SslHandler newSSLHandler(SocketChannel channel) {
        throw new AssertionError("This class implements RemoteEndpointAwareSSLOptions, this method should not be called");
    }

    @Override
    public SslHandler newSSLHandler(SocketChannel channel, InetSocketAddress remoteEndpoint) {
        return context.newHandler(channel.alloc(), remoteEndpoint.getHostName(), remoteEndpoint.getPort());
    }
}
