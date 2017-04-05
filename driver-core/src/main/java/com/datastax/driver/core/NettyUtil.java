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

import com.google.common.base.Throwables;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Locale;
import java.util.concurrent.ThreadFactory;

/**
 * A set of utilities related to the underlying Netty layer.
 */
@SuppressWarnings("unchecked")
class NettyUtil {

    private static final boolean FORCE_NIO = SystemProperties.getBoolean("com.datastax.driver.FORCE_NIO", false);

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyUtil.class);

    private static final boolean SHADED;

    private static final boolean USE_EPOLL;

    private static final Constructor<? extends EventLoopGroup> EPOLL_EVENT_LOOP_GROUP_CONSTRUCTOR;

    private static final Class<? extends SocketChannel> EPOLL_CHANNEL_CLASS;

    private static final Class[] EVENT_GROUP_ARGUMENTS = {int.class, ThreadFactory.class};

    static {
        boolean shaded;
        try {
            // prevent this string from being shaded
            Class.forName(String.format("%s.%s.channel.Channel", "io", "netty"));
            shaded = false;
        } catch (ClassNotFoundException e) {
            try {
                Class.forName("com.datastax.shaded.netty.channel.Channel");
                shaded = true;
            } catch (ClassNotFoundException e1) {
                throw new AssertionError("Cannot locate Netty classes in the classpath:" + e1);
            }
        }
        SHADED = shaded;
        boolean useEpoll = false;
        if (!SHADED) {
            try {
                Class<?> epoll = Class.forName("io.netty.channel.epoll.Epoll");
                if (FORCE_NIO) {
                    LOGGER.info("Found Netty's native epoll transport in the classpath, "
                            + "but NIO was forced through the FORCE_NIO system property.");
                } else if (!System.getProperty("os.name", "").toLowerCase(Locale.US).equals("linux")) {
                    LOGGER.warn("Found Netty's native epoll transport, but not running on linux-based operating " +
                            "system. Using NIO instead.");
                } else if (!(Boolean) epoll.getMethod("isAvailable").invoke(null)) {
                    LOGGER.warn("Found Netty's native epoll transport in the classpath, but epoll is not available. "
                            + "Using NIO instead.", (Throwable) epoll.getMethod("unavailabilityCause").invoke(null));
                } else {
                    LOGGER.info("Found Netty's native epoll transport in the classpath, using it");
                    useEpoll = true;
                }
            } catch (ClassNotFoundException e) {
                LOGGER.info("Did not find Netty's native epoll transport in the classpath, defaulting to NIO.");
            } catch (Exception e) {
                LOGGER.warn("Unexpected error trying to find Netty's native epoll transport in the classpath, defaulting to NIO.", e);
            }
        } else {
            LOGGER.info("Detected shaded Netty classes in the classpath; native epoll transport will not work properly, "
                    + "defaulting to NIO.");
        }
        USE_EPOLL = useEpoll;
        Constructor<? extends EventLoopGroup> constructor = null;
        Class<? extends SocketChannel> channelClass = null;
        if (USE_EPOLL) {
            try {
                channelClass = (Class<? extends SocketChannel>) Class.forName("io.netty.channel.epoll.EpollSocketChannel");
                Class<?> epoolEventLoupGroupClass = Class.forName("io.netty.channel.epoll.EpollEventLoopGroup");
                constructor = (Constructor<? extends EventLoopGroup>) epoolEventLoupGroupClass.getDeclaredConstructor(EVENT_GROUP_ARGUMENTS);
            } catch (Exception e) {
                throw new AssertionError("Netty's native epoll is in use but cannot locate Epoll classes, this should not happen: " + e);
            }
        }
        EPOLL_EVENT_LOOP_GROUP_CONSTRUCTOR = constructor;
        EPOLL_CHANNEL_CLASS = channelClass;
    }

    /**
     * @return true if the current driver bundle is using shaded Netty classes, false otherwise.
     */

    public static boolean isShaded() {
        return SHADED;
    }

    /**
     * @return true if native epoll transport is available in the classpath, false otherwise.
     */
    public static boolean isEpollAvailable() {
        return USE_EPOLL;
    }

    /**
     * Return a new instance of {@link EventLoopGroup}.
     * <p/>
     * Returns an instance of {@link io.netty.channel.epoll.EpollEventLoopGroup} if {@link #isEpollAvailable() epoll is available},
     * or an instance of {@link NioEventLoopGroup} otherwise.
     *
     * @param factory the {@link ThreadFactory} instance to use to create the new instance of {@link EventLoopGroup}
     * @return a new instance of {@link EventLoopGroup}
     */
    public static EventLoopGroup newEventLoopGroupInstance(ThreadFactory factory) {
        if (isEpollAvailable()) {
            try {
                return EPOLL_EVENT_LOOP_GROUP_CONSTRUCTOR.newInstance(0, factory);
            } catch (Exception e) {
                throw Throwables.propagate(e); // should not happen
            }
        } else {
            return new NioEventLoopGroup(0, factory);
        }
    }

    /**
     * Return the SocketChannel class to use.
     * <p/>
     * Returns an instance of {@link io.netty.channel.epoll.EpollSocketChannel} if {@link #isEpollAvailable() epoll is available},
     * or an instance of {@link NioSocketChannel} otherwise.
     *
     * @return the SocketChannel class to use.
     */
    public static Class<? extends SocketChannel> channelClass() {
        if (isEpollAvailable()) {
            return EPOLL_CHANNEL_CLASS;
        } else {
            return NioSocketChannel.class;
        }
    }

}
