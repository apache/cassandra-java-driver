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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableList;

/**
 * A Cassandra node.
 *
 * This class keeps the information the driver maintain on a given Cassandra node.
 */
public class Host {

    private final InetAddress address;

    private volatile String datacenter;
    private volatile String rack;

    private volatile boolean isUp;
    private final ConvictionPolicy policy;

    // Tracks reconnection attempts to that host so we avoid adding multiple tasks
    final AtomicReference<ScheduledFuture<?>> reconnectionAttempt = new AtomicReference<ScheduledFuture<?>>();

    final ExecutionInfo defaultExecutionInfo;

    // ClusterMetadata keeps one Host object per inet address, so don't use
    // that constructor unless you know what you do (use ClusterMetadata.getHost typically).
    Host(InetAddress address, ConvictionPolicy.Factory policy) {
        if (address == null || policy == null)
            throw new NullPointerException();

        this.address = address;
        this.policy = policy.create(this);
        this.defaultExecutionInfo = new ExecutionInfo(ImmutableList.of(this));
    }

    void setLocationInfo(String datacenter, String rack) {
        this.datacenter = datacenter;
        this.rack = rack;
    }

    /**
     * Returns the node address.
     *
     * @return the node {@link InetAddress}.
     */
    public InetAddress getAddress() {
        return address;
    }

    /**
     * Returns the name of the datacenter this host is part of.
     *
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
     *
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
     * Returns whether the host is considered up by the driver.
     * <p>
     * Please note that this is only the view of the driver may not reflect the
     * reality. In particular a node can be down but the driver hasn't detected
     * it yet, or he can have been restarted but the driver hasn't detected it
     * yet (in particular, for hosts to which the driver does not connect (because
     * the {@code LoadBalancingPolicy.distance} method says so), this information
     * may be durably inaccurate). This information should thus only be
     * considered as best effort and should not be relied upon too strongly.
     *
     * @return whether the node is considered up.
     */
    public boolean isUp() {
        return isUp;
    }

    @Override
    public final int hashCode() {
        return address.hashCode();
    }

    @Override
    public final boolean equals(Object o) {
        if(!(o instanceof Host))
            return false;

        return address.equals(((Host)o).address);
    }

    @Override
    public String toString() {
        return address.toString();
    }

    void setDown() {
        isUp = false;
    }

    void setUp() {
        policy.reset();
        isUp = true;
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
