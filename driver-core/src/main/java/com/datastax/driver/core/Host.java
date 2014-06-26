/*
 *      Copyright (C) 2012 DataStax Inc.
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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.Host.State.*;

/**
 * A Cassandra node.
 *
 * This class keeps the information the driver maintain on a given Cassandra node.
 */
public class Host {

    private static final Logger logger = LoggerFactory.getLogger(Host.class);

    private final InetSocketAddress address;

    enum State { ADDED, DOWN, UP }
    private volatile State state;
    private final ConvictionPolicy policy;

    // Tracks reconnection attempts to that host so we avoid adding multiple tasks
    final AtomicReference<ScheduledFuture<?>> reconnectionAttempt = new AtomicReference<ScheduledFuture<?>>();

    final ExecutionInfo defaultExecutionInfo;

    private volatile String datacenter;
    private volatile String rack;
    private volatile VersionNumber cassandraVersion;

    // The listen_address (really, the broadcast one) as know by Cassandra. We use that internally because
    // that's the 'peer' in the 'System.peers' table and avoids querying the full peers table in
    // ControlConnection.refreshNodeInfo. We don't want to expose however because we don't always have the info
    // (partly because the 'System.local' doesn't have it for some weird reason for instance).
    volatile InetAddress listenAddress;

    // ClusterMetadata keeps one Host object per inet address and we rely on this (more precisely,
    // we rely on the fact that we can use Object equality as a valid equality), so don't use
    // that constructor but ClusterMetadata.getHost instead.
    Host(InetSocketAddress address, ConvictionPolicy.Factory policy) {
        if (address == null || policy == null)
            throw new NullPointerException();

        this.address = address;
        this.policy = policy.create(this);
        this.defaultExecutionInfo = new ExecutionInfo(ImmutableList.of(this));
        this.state = ADDED;
    }

    void setLocationInfo(String datacenter, String rack) {
        this.datacenter = datacenter;
        this.rack = rack;
    }

    void setVersionAndListenAdress(String cassandraVersion, InetAddress listenAddress) {
        if (listenAddress != null)
            this.listenAddress = listenAddress;

        if (cassandraVersion == null)
            return;
        try {
            this.cassandraVersion = VersionNumber.parse(cassandraVersion);
        } catch (IllegalArgumentException e) {
            logger.warn("Error parsing Cassandra version {}. This shouldn't have happened", cassandraVersion);
        }
    }

    /**
     * Returns the node address.
     * <p>
     * This is a shortcut for {@code getSocketAddress().getAddress()}.
     *
     * @return the node {@link InetAddress}.
     */
    public InetAddress getAddress() {
        return address.getAddress();
    }

    /**
     * Returns the node socket address.
     *
     * @return the node {@link InetSocketAddress}.
     */
    public InetSocketAddress getSocketAddress() {
        return address;
    }

    /**
     * Returns the name of the datacenter this host is part of.
     * <p>
     * The returned datacenter name is the one as known by Cassandra.
     * It is also possible for this information to be unavailable. In that
     * case this method returns {@code null}, and the caller should always be aware
     * of this possibility.
     *
     * @return the Cassandra datacenter name or null if datacenter is unavailable.
     */
    public String getDatacenter() {
        return datacenter;
    }

    /**
     * Returns the name of the rack this host is part of.
     * <p>
     * The returned rack name is the one as known by Cassandra.
     * It is also possible for this information to be unavailable. In that case
     * this method returns {@code null}, and the caller should always aware of this
     * possibility.
     *
     * @return the Cassandra rack name or null if the rack is unavailable
     */
    public String getRack() {
        return rack;
    }

    /**
     * The Cassandra version the host is running.
     * <p>
     * As for other host information fetch from Cassandra above, the returned
     * version can theoretically be null if the information is unavailable.
     *
     * @return the Cassandra version the host is running.
     */
    public VersionNumber getCassandraVersion() {
        return cassandraVersion;
    }

    /**
     * Returns whether the host is considered up by the driver.
     * <p>
     * Please note that this is only the view of the driver and may not reflect
     * reality. In particular a node can be down but the driver hasn't detected
     * it yet, or it can have been restarted and the driver hasn't detected it
     * yet (in particular, for hosts to which the driver does not connect (because
     * the {@code LoadBalancingPolicy.distance} method says so), this information
     * may be durably inaccurate). This information should thus only be
     * considered as best effort and should not be relied upon too strongly.
     *
     * @return whether the node is considered up.
     */
    public boolean isUp() {
        return state == UP;
    }

    boolean wasJustAdded() {
        return state == ADDED;
    }

    @Override
    public String toString() {
        return address.toString();
    }

    void setDown() {
        state = DOWN;
    }

    void setUp() {
        policy.reset();
        state = UP;
    }

    boolean signalConnectionFailure(ConnectionException exception) {
        return policy.addFailure(exception);
    }

    /**
     * Interface for listeners that are interested in hosts added, up, down and
     * removed events.
     * <p>
     * It is possible for the same event to be fired multiple times,
     * particularly for up or down events. Therefore, a listener should
     * ignore the same event if it has already been notified of a
     * node's state.
     */
    public interface StateListener {

        /**
         * Called when a new node is added to the cluster.
         * <p>
         * The newly added node should be considered up.
         *
         * @param host the host that has been newly added.
         */
        public void onAdd(Host host);

        /**
         * Called when a node is determined to be up.
         *
         * @param host the host that has been detected up.
         */
        public void onUp(Host host);

        /**
         * Called when a node is determined to be down.
         *
         * @param host the host that has been detected down.
         */
        public void onDown(Host host);

        /**
         * Called when a node is removed from the cluster.
         *
         * @param host the removed host.
         */
        public void onRemove(Host host);
    }
}
