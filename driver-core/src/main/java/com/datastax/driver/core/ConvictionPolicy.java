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

import com.datastax.driver.core.policies.ReconnectionPolicy;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * The policy with which to decide whether a host should be considered down.
 * <p/>
 * TODO: this class is fully abstract (rather than an interface) because I'm
 * not sure it's worth exposing (and if we do expose it, we need to expose
 * ConnectionException). Maybe just exposing say a threshold of error before
 * convicting a node is enough.
 */
abstract class ConvictionPolicy {

    /**
     * Called when new connections to the host are about to be created.
     *
     * @param count the number of connections
     */
    abstract void signalConnectionsOpening(int count);

    /**
     * Called when a connection closed normally.
     */
    abstract void signalConnectionClosed(Connection connection);

    /**
     * Called when a connection error occurs on a connection to the host this policy applies to.
     *
     * @return whether the host should be considered down.
     */
    abstract boolean signalConnectionFailure(Connection connection);

    abstract boolean canReconnectNow();

    /**
     * Simple factory interface to allow creating {@link ConvictionPolicy} instances.
     */
    interface Factory {

        /**
         * Creates a new ConvictionPolicy instance for {@code host}.
         *
         * @param host the host this policy applies to
         * @return the newly created {@link ConvictionPolicy} instance.
         */
        ConvictionPolicy create(Host host, ReconnectionPolicy reconnectionPolicy);
    }

    static class DefaultConvictionPolicy extends ConvictionPolicy {
        private final Host host;
        private final ReconnectionPolicy reconnectionPolicy;
        private final AtomicInteger openConnections = new AtomicInteger();

        private volatile long nextReconnectionTime = Long.MIN_VALUE;
        private ReconnectionPolicy.ReconnectionSchedule reconnectionSchedule;

        private DefaultConvictionPolicy(Host host, ReconnectionPolicy reconnectionPolicy) {
            this.host = host;
            this.reconnectionPolicy = reconnectionPolicy;
        }

        @Override
        void signalConnectionsOpening(int count) {
            int newTotal = openConnections.addAndGet(count);
            Host.statesLogger.debug("[{}] preparing to open {} new connections, total = {}", host, count, newTotal);
            resetReconnectionTime();
        }

        @Override
        void signalConnectionClosed(Connection connection) {
            int remaining = openConnections.decrementAndGet();
            assert remaining >= 0;
            Host.statesLogger.debug("[{}] {} closed, remaining = {}", host, connection, remaining);
        }

        @Override
        boolean signalConnectionFailure(Connection connection) {
            if (host.state != Host.State.DOWN)
                updateReconnectionTime();

            int remaining = openConnections.decrementAndGet();
            assert remaining >= 0;
            Host.statesLogger.debug("[{}] {} failed, remaining = {}", host, connection, remaining);
            return remaining == 0;
        }

        private synchronized void updateReconnectionTime() {
            long now = System.nanoTime();
            if (nextReconnectionTime > now)
                // Someone else updated the time before us
                return;

            if (reconnectionSchedule == null)
                reconnectionSchedule = reconnectionPolicy.newSchedule();

            long nextDelayMs = reconnectionSchedule.nextDelayMs();
            Host.statesLogger.debug("[{}] preventing new connections for the next {} ms", host, nextDelayMs);
            nextReconnectionTime = now + NANOSECONDS.convert(nextDelayMs, MILLISECONDS);
        }

        private synchronized void resetReconnectionTime() {
            reconnectionSchedule = null;
            nextReconnectionTime = Long.MIN_VALUE;
        }

        @Override
        boolean canReconnectNow() {
            boolean canReconnectNow = nextReconnectionTime == Long.MIN_VALUE ||
                    System.nanoTime() >= nextReconnectionTime;
            Host.statesLogger.trace("canReconnectNow={}", canReconnectNow);
            return canReconnectNow;
        }

        static class Factory implements ConvictionPolicy.Factory {

            @Override
            public ConvictionPolicy create(Host host, ReconnectionPolicy reconnectionPolicy) {
                return new DefaultConvictionPolicy(host, reconnectionPolicy);
            }
        }
    }
}
