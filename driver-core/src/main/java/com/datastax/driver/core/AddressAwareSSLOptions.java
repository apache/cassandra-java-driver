/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.net.InetSocketAddress;

import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;

/**
 * Defines how the driver configures SSL connections.
 * 
 * Child interface to SSLOptions with the possibility to pass remote endpoint data
 * when instantiating SSLHandlers (and, by extension, SSLContext). This is needed
 * when hostname verification is required, for example. The reason this is a
 * child interface is to keep backward compatibility alive for SSLContext. This 
 * interface maybe be merged into SSLOptions in a later major release.
 * 
 * @see <a href="https://datastax-oss.atlassian.net/browse/JAVA-1364">JAVA-1364</a>
 */
public interface AddressAwareSSLOptions extends SSLOptions {

    SslHandler newSSLHandler(SocketChannel channel, InetSocketAddress address);

}
