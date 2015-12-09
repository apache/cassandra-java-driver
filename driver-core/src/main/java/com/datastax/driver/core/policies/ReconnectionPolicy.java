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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;

/**
 * Policy that decides how often the reconnection to a dead node is attempted.
 * <p/>
 * Each time a node is detected dead (because a connection error occurs), a new
 * {@code ReconnectionSchedule} instance is created (through the {@link #newSchedule()}).
 * Then each call to the {@link ReconnectionSchedule#nextDelayMs} method of
 * this instance will decide when the next reconnection attempt to this node
 * will be tried.
 * <p/>
 * Note that if the driver receives a push notification from the Cassandra cluster
 * that a node is UP, any existing {@code ReconnectionSchedule} on that node
 * will be cancelled and a new one will be created (in effect, the driver reset
 * the scheduler).
 * <p/>
 * The default {@link ExponentialReconnectionPolicy} policy is usually
 * adequate.
 */
public interface ReconnectionPolicy {

    /**
     * Creates a new schedule for reconnection attempts.
     *
     * @return the created schedule.
     */
    public ReconnectionSchedule newSchedule();

    /**
     * Schedules reconnection attempts to a node.
     */
    public interface ReconnectionSchedule {

        /**
         * When to attempt the next reconnection.
         * <p/>
         * This method will be called once when the host is detected down to
         * schedule the first reconnection attempt, and then once after each failed
         * reconnection attempt to schedule the next one. Hence each call to this
         * method are free to return a different value.
         *
         * @return a time in milliseconds to wait before attempting the next
         * reconnection.
         */
        public long nextDelayMs();
    }

    /**
     * Gets invoked at cluster startup.
     *
     * @param cluster the cluster that this policy is associated with.
     */
    void init(Cluster cluster);

    /**
     * Gets invoked at cluster shutdown.
     * <p/>
     * This gives the policy the opportunity to perform some cleanup, for instance stop threads that it might have started.
     */
    void close();
}
