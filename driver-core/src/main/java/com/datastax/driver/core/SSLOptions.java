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

import java.net.InetSocketAddress;

/**
 * Defines how the driver configures SSL connections.
 * <p/>
 * Note: since version 3.2.0, users are encouraged to implement
 * {@link RemoteEndpointAwareSSLOptions} instead.
 *
 * @see RemoteEndpointAwareSSLOptions
 * @see JdkSSLOptions
 * @see NettySSLOptions
 */
@SuppressWarnings("deprecation")
public interface SSLOptions {

    /**
     * Creates a new SSL handler for the given Netty channel.
     * <p/>
     * This gets called each time the driver opens a new connection to a Cassandra host. The newly created handler will be added
     * to the channel's pipeline to provide SSL support for the connection.
     * <p/>
     * You don't necessarily need to implement this method directly; see the provided implementations: {@link JdkSSLOptions} and
     * {@link NettySSLOptions}.
     *
     * @param channel the channel.
     * @return the handler.
     * @deprecated use {@link RemoteEndpointAwareSSLOptions#newSSLHandler(SocketChannel, InetSocketAddress)} instead.
     *
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    SslHandler newSSLHandler(SocketChannel channel);
}
