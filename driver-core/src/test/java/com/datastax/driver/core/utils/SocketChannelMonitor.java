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
package com.datastax.driver.core.utils;

import com.datastax.driver.core.NettyOptions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility that observes {@link SocketChannel}s.  Helpful for ensuring that Sockets are actually closed
 * when they should be.  Utilizes {@link NettyOptions} to monitor created {@link SocketChannel}s.
 */
public class SocketChannelMonitor implements Runnable, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(SocketChannelMonitor.class);

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("SocketMonitor-%d").build());

    // use a weak set so channels may be garbage collected.
    private final Collection<SocketChannel> channels = Collections.newSetFromMap(
            new MapMaker().weakKeys().<SocketChannel, Boolean>makeMap());

    private final AtomicLong channelsCreated = new AtomicLong(0);

    private final NettyOptions nettyOptions = new NettyOptions() {
        @Override
        public void afterChannelInitialized(SocketChannel channel) throws Exception {
            channels.add(channel);
            channelsCreated.incrementAndGet();
        }

        @Override
        public void onClusterClose(EventLoopGroup eventLoopGroup) {
            eventLoopGroup.shutdownGracefully(0, 15, TimeUnit.SECONDS).syncUninterruptibly();
        }
    };

    @Override
    public void run() {
        try {
            report();
        } catch (Exception e) {
            logger.error("Error countered.", e);
        }
    }

    @Override
    public void close() throws IOException {
        stop();
    }

    public void stop() {
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ok
        }
    }

    /**
     * @return A custom {@link NettyOptions} instance that hooks into afterChannelInitialized added channels may be
     * monitored.
     */
    public NettyOptions nettyOptions() {
        return nettyOptions;
    }

    public static Predicate<SocketChannel> openChannels = new Predicate<SocketChannel>() {
        @Override
        public boolean apply(SocketChannel input) {
            return input.isOpen();
        }
    };

    /**
     * Schedules a {@link #report()} to be called every configured interval.
     *
     * @param interval how often to report.
     * @param timeUnit at what time precision to report at.
     */
    public void reportAtFixedInterval(int interval, TimeUnit timeUnit) {
        executor.scheduleAtFixedRate(this, interval, interval, timeUnit);
    }

    /**
     * Reports for all sockets.
     */
    public void report() {
        report(Predicates.<SocketChannel>alwaysTrue());
    }

    /**
     * <p/>
     * Report for all sockets matching the given predicate.  The report format reflects the number of open, closed,
     * live and total sockets created.  This is logged at DEBUG if enabled.
     * <p/>
     * <p/>
     * If TRACE is enabled, each individual socket will be logged as well.
     *
     * @param channelFilter used to determine which sockets to report on.
     */
    public void report(Predicate<SocketChannel> channelFilter) {
        if (logger.isDebugEnabled()) {
            Iterable<SocketChannel> channels = matchingChannels(channelFilter);
            Iterable<SocketChannel> open = Iterables.filter(channels, openChannels);
            Iterable<SocketChannel> closed = Iterables.filter(channels, Predicates.not(openChannels));

            logger.debug("Channel states: {} open, {} closed, live {}, total sockets created " +
                            "(including those that don't match filter) {}.",
                    Iterables.size(open),
                    Iterables.size(closed),
                    Iterables.size(channels),
                    channelsCreated.get());

            if (logger.isTraceEnabled()) {
                logger.trace("Open channels {}.", open);
                logger.trace("Closed channels {}.", closed);
            }
        }
    }

    private static Comparator<SocketChannel> BY_REMOTE_ADDRESS = new Comparator<SocketChannel>() {
        @Override
        public int compare(SocketChannel t0, SocketChannel t1) {
            // Should not be null as these are filtered previously in matchingChannels.
            assert t0 != null && t0.remoteAddress() != null;
            assert t1 != null && t1.remoteAddress() != null;
            return t0.remoteAddress().toString().compareTo(t1.remoteAddress().toString());
        }
    };

    public Collection<SocketChannel> openChannels(InetSocketAddress... addresses) {
        return openChannels(Arrays.asList(addresses));
    }

    /**
     * @param addresses The addresses to include.
     * @return Open channels matching the given socket addresses.
     */
    public Collection<SocketChannel> openChannels(final Collection<InetSocketAddress> addresses) {
        List<SocketChannel> channels = Lists.newArrayList(matchingChannels(new Predicate<SocketChannel>() {
            @Override
            public boolean apply(SocketChannel input) {
                return input.isOpen() && input.remoteAddress() != null && addresses.contains(input.remoteAddress());
            }
        }));
        Collections.sort(channels, BY_REMOTE_ADDRESS);
        return channels;
    }

    /**
     * @param channelFilter {@link Predicate} to use to determine whether or not a socket shall be considered.
     * @return Channels matching the given {@link Predicate}.
     */
    public Iterable<SocketChannel> matchingChannels(final Predicate<SocketChannel> channelFilter) {
        return Iterables.filter(Lists.newArrayList(channels), Predicates.and(Predicates.notNull(), channelFilter));
    }

}
