/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

import io.netty.channel.socket.SocketChannel;

/**
 * A set of hooks that allow clients to customize the driver's underlying Netty layer.
 * <p>
 * Clients that need to plug-in their own logic into the driver's underlying Netty layer can
 * subclass this class and provide the necessary customization through any of the
 * publicly available methods.
 * <p>
 * Typically, clients would register this class with Cluster.Builder:
 *
 * <pre>
 *     ChannelCustomizer customizer = ...
 *     Cluster cluster = Cluster.builder()
 *          .addContactPoint(...)
 *          .withNettyCustomizer(customizer)
 *          .build();
 * </pre>
 *
 * <p>
 * <strong>Warning:</strong> since 2.0.9 and 2.1.4 (see JAVA-538),
 * the driver is available in two different flavors: with a standard Maven dependency on Netty,
 * or with a "shaded" (internalized) Netty dependency.
 * <p>
 * However, this API exposes Netty classes (SocketChannel, etc.) to client applications;
 * as a consequence, <em>it can only be extended by clients if they are using the non-shaded
 * version of driver</em>.
 *
 * @jira_ticket JAVA-640
 * @since 2.0.10
 */
public class NettyCustomizer {

    public static final NettyCustomizer DEFAULT_INSTANCE = new NettyCustomizer();

    /**
     * Invoked after the driver has initialized the given {@Link SocketChannel} instance;
     * in particular, this is called after the driver has added its default {@link io.netty.channel.ChannelHandler}s handlers to it.
     *
     * @param channel the {@Link SocketChannel} instance, after being initialized by the driver
     * @throws Exception if this methods encounters any errors
     */
    public void afterChannelInitialized(SocketChannel channel) throws Exception {
        //noop
    }

}
