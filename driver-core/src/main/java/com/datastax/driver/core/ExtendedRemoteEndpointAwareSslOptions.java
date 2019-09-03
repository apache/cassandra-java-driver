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

import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;

public interface ExtendedRemoteEndpointAwareSslOptions extends RemoteEndpointAwareSSLOptions {

  /**
   * Creates a new SSL handler for the given Netty channel and the given remote endpoint.
   *
   * <p>This gets called each time the driver opens a new connection to a Cassandra host. The newly
   * created handler will be added to the channel's pipeline to provide SSL support for the
   * connection.
   *
   * <p>You don't necessarily need to implement this method directly; see the provided
   * implementations: {@link RemoteEndpointAwareJdkSSLOptions} and {@link
   * RemoteEndpointAwareNettySSLOptions}.
   *
   * @param channel the channel.
   * @param remoteEndpoint the remote endpoint information.
   * @return a newly-created {@link SslHandler}.
   */
  SslHandler newSSLHandler(SocketChannel channel, EndPoint remoteEndpoint);
}
