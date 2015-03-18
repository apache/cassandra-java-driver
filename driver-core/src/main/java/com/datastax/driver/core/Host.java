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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.policies.ReconnectionPolicy;

/**
 * A Cassandra node.
 *
 * This class keeps the information the driver maintain on a given Cassandra node.
 */
public class Host {

    private static final Logger logger = LoggerFactory.getLogger(Host.class);


    private final InetSocketAddress address;

    enum State { ADDED, DOWN, SUSPECT, UP }
    volatile State state;
    /** Ensures state change notifications for that host are handled serially */
    final ReentrantLock notificationsLock = new ReentrantLock(true);

    private final ConvictionPolicy policy;
    private final Cluster.Manager manager;

    // Tracks the first "immediate" reconnection attempt when a node get suspected.
    final AtomicReference<ListenableFuture<?>> initialReconnectionAttempt = new AtomicReference<ListenableFuture<?>>(Futures.immediateFuture(null));

    // Tracks later reconnection attempts to that host so we avoid adding multiple tasks.
    final AtomicReference<ListenableFuture<?>> reconnectionAttempt = new AtomicReference<ListenableFuture<?>>();

    final ExecutionInfo defaultExecutionInfo;

    private volatile String datacenter;
    private volatile String rack;
    private volatile VersionNumber cassandraVersion;

    // The listen_address (really, the broadcast one) as know by Cassandra. We use that internally because
    // that's the 'peer' in the 'System.peers' table and avoids querying the full peers table in
    // ControlConnection.refreshNodeInfo. We don't want to expose however because we don't always have the info
    // (partly because the 'System.local' doesn't have it for some weird reason for instance).
    volatile InetAddress listenAddress;

    private volatile Set<Token> tokens;

    // ClusterMetadata keeps one Host object per inet address and we rely on this (more precisely,
    // we rely on the fact that we can use Object equality as a valid equality), so don't use
    // that constructor but ClusterMetadata.getHost instead.
    Host(InetSocketAddress address, ConvictionPolicy.Factory policy, Cluster.Manager manager) {
        if (address == null || policy == null)
            throw new NullPointerException();

        this.address = address;
        this.policy = policy.create(this);
        this.manager = manager;
        this.defaultExecutionInfo = new ExecutionInfo(ImmutableList.of(this));
        this.state = State.ADDED;
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
     * Returns the tokens that this host owns.
     *
     * @return the (immutable) set of tokens.
     */
    public Set<Token> getTokens() {
        return tokens;
    }

    void setTokens(Set<Token> tokens) {
        this.tokens = tokens;
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
        // Consider a suspected host UP until proved otherwise to avoid
        // having the status flapping if it turns out the host is not really down.
        return state == State.UP || state == State.SUSPECT;
    }

    /**
     * Returns a {@code ListenableFuture} representing the completion of the first
     * reconnection attempt after a node has been suspected.
     * <p>
     * This is useful in load balancing policies when there are no more live nodes and
     * we are trying suspected nodes.
     *
     * @return the future.
     */
    public ListenableFuture<?> getInitialReconnectionAttemptFuture() {
        return initialReconnectionAttempt.get();
    }

    /**
     * Returns a {@code ListenableFuture} representing the completion of the reconnection
     * attempts scheduled after a host is marked {@code DOWN}.
     * <p>
     * <b>If the caller cancels this future,</b> the driver will not try to reconnect to
     * this host until it receives an UP event for it. Note that this could mean never, if
     * the node was marked down because of a driver-side error (e.g. read timeout) but no
     * failure was detected by Cassandra. The caller might decide to trigger an explicit
     * reconnection attempt at a later point with {@link #tryReconnectOnce()}.
     *
     * @return the future, or {@code null} if no reconnection attempt was in progress.
     */
    public ListenableFuture<?> getReconnectionAttemptFuture() {
        return reconnectionAttempt.get();
    }

    /**
     * Triggers an asynchronous reconnection attempt to this host.
     * <p>
     * This method is intended for load balancing policies that mark hosts as {@link HostDistance#IGNORED IGNORED},
     * but still need a way to periodically check these hosts' states (UP / DOWN).
     * <p>
     * For a host that is at distance {@code IGNORED}, this method will try to reconnect exactly once: if
     * reconnection succeeds, the host is marked {@code UP}; otherwise, no further attempts will be scheduled.
     * It has no effect if the node is already {@code UP}, or if a reconnection attempt is already in progress.
     * <p>
     * Note that if the host is <em>not</em> a distance {@code IGNORED}, this method <em>will</em> trigger a periodic
     * reconnection attempt if the reconnection fails.
     */
    public void tryReconnectOnce() {
        this.manager.startSingleReconnectionAttempt(this);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Host) {
            Host that = (Host)other;
            return this.address.equals(that.address);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return address.hashCode();
    }

    boolean wasJustAdded() {
        return state == State.ADDED;
    }

    @Override
    public String toString() {
        return address.toString();
    }

    void setDown() {
        state = State.DOWN;
    }

    void setUp() {
        policy.reset();
        state = State.UP;
    }

    boolean setSuspected() {
        if (state != State.UP)
            return false;

        state = State.SUSPECT;
        return true;
    }

    boolean signalConnectionFailure(ConnectionException exception) {
        return policy.addFailure(exception);
    }

    /**
     * Interface for listeners that are interested in hosts added, up, down and
     * removed events.
     * <p>
     * It is possible for the same event to be fired multiple times,
     * particularly for up or down events. Therefore, a listener should ignore
     * the same event if it has already been notified of a node's state.
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
         * Called when a node is suspected to be dead.
         * <p>
         * A node is suspected to be dead when an error occurs on one of it's
         * opened connection. As soon as an host is suspected, a connection attempt
         * to that host is immediately tried. If this succeed, then it means that
         * the connection was disfunctional but that the node was not really down.
         * If this fails however, this means the node is truly dead, onDown() is
         * called and further reconnection attempts are scheduled according to the
         * {@link com.datastax.driver.core.policies.ReconnectionPolicy} in place.
         * <p>
         * When this event is triggered, it is possible to call the host
         * {@link #getInitialReconnectionAttemptFuture} method to wait until the
         * initial and immediate reconnection attempt succeed or fail.
         * <p>
         * Note that some StateListener may ignore that event. If a node that
         * that is suspected down turns out to be truly down (that is, the driver
         * cannot successfully connect to it right away), then {@link #onDown} will
         * be called.
         */
        public void onSuspected(Host host);

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
