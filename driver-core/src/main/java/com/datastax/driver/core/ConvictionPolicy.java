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

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.datastax.driver.core.policies.ReconnectionPolicy;

/**
 * The policy with which to decide whether a host should be considered down.
 *
 * TODO: this class is fully abstract (rather than an interface) because I'm
 * not sure it's worth exposing (and if we do expose it, we need to expose
 * ConnectionException). Maybe just exposing say a threshold of error before
 * convicting a node is enough.
 */
abstract class ConvictionPolicy {

    /**
     * Called when a new connection to the host has been successfully opened.
     */
    abstract void signalConnectionCreated();

    /**
     * Called when a connection closed normally.
     */
    abstract void signalConnectionClosed();

    /**
     * Called when a connection error occurs on a connection to the host this policy applies to.
     *
     * @param exception the connection error that occurred.
     * @param connectionInitialized whether the connection was initialized.
     * @return whether the host should be considered down.
     */
    abstract boolean signalConnectionFailure(ConnectionException exception, boolean connectionInitialized);

    abstract boolean canReconnectNow();

    /**
     * Called when the host goes down.
     */
    abstract void reset();

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
        void signalConnectionCreated() {
            int newCount = openConnections.incrementAndGet();
            Host.statesLogger.debug("[{}] new connection created, total = {}", host, newCount);
            resetReconnectionTime();
        }

        @Override
        void signalConnectionClosed() {
            if (host.state == Host.State.DOWN)
                return;
            int remaining = openConnections.decrementAndGet();
            assert remaining >= 0;
            Host.statesLogger.debug("[{}] connection closed, remaining = {}", host, remaining);
        }

        @Override
        boolean signalConnectionFailure(ConnectionException exception, boolean wasFullyInitialized) {
            if (host.state == Host.State.DOWN)
                return false;

            updateReconnectionTime();
            int remaining = (wasFullyInitialized)
                ? openConnections.decrementAndGet()
                : openConnections.get();

            assert remaining >= 0;
            Host.statesLogger.debug("[{}] remaining connections = {}", host, remaining);
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
            return nextReconnectionTime == Long.MIN_VALUE ||
                System.nanoTime() >= nextReconnectionTime;
        }

        @Override
        synchronized void reset() {
            openConnections.set(0);
            resetReconnectionTime();
        }

        static class Factory implements ConvictionPolicy.Factory {

            @Override
            public ConvictionPolicy create(Host host, ReconnectionPolicy reconnectionPolicy) {
                return new DefaultConvictionPolicy(host, reconnectionPolicy);
            }
        }
    }
}
