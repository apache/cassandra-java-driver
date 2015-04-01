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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * A set of options, or rather hooks, that allow clients to customize the driver's underlying Netty layer.
 * <p>
 * Clients that need to hook into the driver's underlying Netty layer can
 * subclass this class and provide the necessary customization by overriding
 * its methods.
 * <p>
 * Typically, clients would register this class with {@link Cluster#builder()}:
 *
 * <pre>
 *     NettyOptions nettyOptions = ...
 *     Cluster cluster = Cluster.builder()
 *          .addContactPoint(...)
 *          .withNettyOptions(nettyOptions)
 *          .build();
 * </pre>
 *
 * <strong>Extending the NettyOptions API</strong>
 * <p>
 * Contrary to other driver options, the options available in this class should
 * be considered as advanced features and as such, <em>they should only be
 * modified by expert users</em>.
 * <p>
 * <strong>A misconfiguration introduced by the means of this API can have unexpected results
 * and cause the driver to completely fail to connect.</strong>
 * <p>
 * Moreover, since versions 2.0.9 and 2.1.4 (see JAVA-538),
 * the driver is available in two different flavors: with a standard Maven dependency on Netty,
 * or with a "shaded" (internalized) Netty dependency.
 * <p>
 * Given that NettyOptions API exposes Netty classes ({@link SocketChannel}, etc.),
 * <em>it should only be extended by clients using the non-shaded
 * version of driver</em>.
 * <p>
 * <strong>Extending this API with shaded Netty classes is not supported,
 * and in particular for OSGi applications, it is likely that such a configuration would lead to
 * compile and/or runtime errors.</strong>
 *
 * @jira_ticket JAVA-640
 * @since 2.0.10
 */
public class NettyOptions {

    /**
     * The default instance of {@link NettyOptions} to use.
     */
    public static final NettyOptions DEFAULT_INSTANCE = new NettyOptions();

    /**
     * Return the {@link EventLoopGroup} instance to use.
     * <p>
     * This hook is invoked only once at {@link Cluster} initialization;
     * the returned instance will be kept in use throughout the cluster lifecycle.
     * <p>
     * Typically, implementors would return a newly-created instance;
     * it is however possible to re-use a shared instance, but in this
     * case implementors should also override {@link #onClusterClose(EventLoopGroup)}
     * to prevent the shared instance to be closed when the cluster is closed.
     * <p>
     * The default implementation returns a new instance of {@link io.netty.channel.epoll.EpollEventLoopGroup}
     * if {@link NettyUtil#isEpollAvailable() epoll is available},
     * or {@link NioEventLoopGroup} otherwise.
     *
     * @param threadFactory The {@link ThreadFactory} to use when creating a new {@link EventLoopGroup} instance;
     *                      The driver will provide its own internal thread factory here.
     *                      It is safe to ignore it and use another thread factory.
     * @return the {@link EventLoopGroup} instance to use.
     */
    public EventLoopGroup eventLoopGroup(ThreadFactory threadFactory) {
        return NettyUtil.newEventLoopGroupInstance(threadFactory);
    }

    /**
     * Return the specific {@link SocketChannel} subclass to use.
     * <p>
     * This hook is invoked only once at {@link Cluster} initialization;
     * the returned instance will then be used each time the driver creates a new {@link Connection}
     * and configures a new instance of {@link Bootstrap} for it.
     * <p>
     * The default implementation returns {@link io.netty.channel.epoll.EpollSocketChannel} if {@link NettyUtil#isEpollAvailable() epoll is available},
     * or {@link NioSocketChannel} otherwise.
     *
     * @return The {@link SocketChannel} subclass to use.
     */
    public Class<? extends SocketChannel> channelClass() {
        return NettyUtil.channelClass();
    }

    /**
     * Hook invoked each time the driver creates a new {@link Connection}
     * and configures a new instance of {@link Bootstrap} for it.
     * <p>
     * This hook is guaranteed to be called <em>after</em> the driver has applied all
     * {@link SocketOptions}s.
     * <p>
     * This is a good place to add extra {@link io.netty.channel.ChannelHandler ChannelOption}s to the boostrap; e.g.
     * to replace the default {@link io.netty.buffer.ByteBufAllocator ByteBufAllocator} implementation with a custom one,
     * do the following:
     *
     * <pre>
     * ByteBufAllocator myCustomByteBufAllocator = ...
     * boostrap.option(ChannelOption.ALLOCATOR, myCustomByteBufAllocator);
     * </pre>
     *
     * @param bootstrap the {@link Bootstrap} being initialized.
     */
    public void afterBootstrapInitialized(Bootstrap bootstrap) {
        //noop
    }

    /**
     * Hook invoked each time the driver creates a new {@link Connection}
     * and initializes the {@link SocketChannel channel}.
     * <p>
     * This hook is guaranteed to be called <em>after</em> the driver has registered
     * all its internal channel handlers, and applied the configured {@link SSLOptions}, if any.
     * <p>
     * This is a good place to add extra {@link io.netty.channel.ChannelHandler ChannelHandler}s
     * to the channel's pipeline; e.g. to add a custom SSL handler to the beginning of the handler chain,
     * do the following:
     *
     * <pre>
     * ChannelPipeline pipeline = channel.pipeline();
     * SSLEngine myCustomSSLEngine = ...
     * SslHandler myCustomSSLHandler = new SslHandler(myCustomSSLEngine);
     * pipeline.addFirst("ssl", myCustomSSLHandler);
     * </pre>
     *
     * Note: if you intend to provide your own SSL implementation,
     * do not enable the driver's built-in {@link SSLOptions} at the same time.
     *
     * @param channel the {@link SocketChannel} instance, after being initialized by the driver.
     * @throws Exception if this methods encounters any errors.
     */
    public void afterChannelInitialized(SocketChannel channel) throws Exception {
        //noop
    }

    /**
     * Hook invoked when the cluster is shutting down after a call to {@link Cluster#close()}.
     * <p>
     * This is guaranteed to be called only after all connections have been individually
     * closed, and their channels closed, and only once per {@link EventLoopGroup} instance.
     * <p>
     * This gives the implementor a chance to close the {@link EventLoopGroup} properly, if required.
     * <p>
     * The default implementation initiates a {@link EventLoopGroup#shutdownGracefully(long, long, TimeUnit) graceful shutdown}
     * of the passed {@link EventLoopGroup} instance with no "quiet period" and a timeout of 15 seconds;
     * then waits uninterruptibly for the shutdown to complete, or the timeout to occur, whichever happens first.
     * <p>
     * Implementation note: if the {@link EventLoopGroup} instance is being shared, or used for other purposes than to
     * coordinate Netty events for the current cluster, than it should not be shut down here;
     * subclasses would have to override this method accordingly to take the appropriate action.
     *
     * @param eventLoopGroup the event loop group used by the cluster being closed
     */
    public void onClusterClose(EventLoopGroup eventLoopGroup) {
        // shutdownGracefully with default parameters employs a quiet period of 2 seconds
        // where in pre-Netty4 versions, closing a cluster instance was very quick (milliseconds).
        // Since we close the channels before shutting down the eventLoopGroup,
        // it is safe to reduce the quiet period to 0 seconds
        eventLoopGroup.shutdownGracefully(0, 15, SECONDS).syncUninterruptibly();
    }


}
