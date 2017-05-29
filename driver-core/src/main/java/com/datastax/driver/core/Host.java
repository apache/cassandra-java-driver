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

import com.datastax.driver.core.policies.AddressTranslator;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Cassandra node.
 * <p/>
 * This class keeps the information the driver maintain on a given Cassandra node.
 */
public class Host {

    private static final Logger logger = LoggerFactory.getLogger(Host.class);

    static final Logger statesLogger = LoggerFactory.getLogger(Host.class.getName() + ".STATES");

    // The address we'll use to connect to the node
    private final InetSocketAddress address;

    // The broadcast_address as known by Cassandra.
    // We use that internally because
    // that's the 'peer' in the 'System.peers' table and avoids querying the full peers table in
    // ControlConnection.refreshNodeInfo.
    private volatile InetAddress broadcastAddress;

    // The listen_address as known by Cassandra.
    // This is usually the same as broadcast_address unless
    // specified otherwise in cassandra.yaml file.
    private volatile InetAddress listenAddress;

    enum State {ADDED, DOWN, UP}

    volatile State state;
    /**
     * Ensures state change notifications for that host are handled serially
     */
    final ReentrantLock notificationsLock = new ReentrantLock(true);

    final ConvictionPolicy convictionPolicy;
    private final Cluster.Manager manager;

    // Tracks later reconnection attempts to that host so we avoid adding multiple tasks.
    final AtomicReference<ListenableFuture<?>> reconnectionAttempt = new AtomicReference<ListenableFuture<?>>();

    final ExecutionInfo defaultExecutionInfo;

    private volatile String datacenter;
    private volatile String rack;
    private volatile VersionNumber cassandraVersion;

    private volatile Set<Token> tokens;

    private volatile String dseWorkload;
    private volatile boolean dseGraphEnabled;
    private volatile VersionNumber dseVersion;

    // ClusterMetadata keeps one Host object per inet address and we rely on this (more precisely,
    // we rely on the fact that we can use Object equality as a valid equality), so don't use
    // that constructor but ClusterMetadata.getHost instead.
    Host(InetSocketAddress address, ConvictionPolicy.Factory convictionPolicyFactory, Cluster.Manager manager) {
        if (address == null || convictionPolicyFactory == null)
            throw new NullPointerException();

        this.address = address;
        this.convictionPolicy = convictionPolicyFactory.create(this, manager.reconnectionPolicy());
        this.manager = manager;
        this.defaultExecutionInfo = new ExecutionInfo(ImmutableList.of(this));
        this.state = State.ADDED;
    }

    void setLocationInfo(String datacenter, String rack) {
        this.datacenter = datacenter;
        this.rack = rack;
    }

    void setVersion(String cassandraVersion) {
        VersionNumber versionNumber = null;
        try {
            if (cassandraVersion != null) {
                versionNumber = VersionNumber.parse(cassandraVersion);
            }
        } catch (IllegalArgumentException e) {
            logger.warn("Error parsing Cassandra version {}. This shouldn't have happened", cassandraVersion);
        }
        this.cassandraVersion = versionNumber;
    }

    void setBroadcastAddress(InetAddress broadcastAddress) {
        this.broadcastAddress = broadcastAddress;
    }

    void setListenAddress(InetAddress listenAddress) {
        this.listenAddress = listenAddress;
    }

    void setDseVersion(String dseVersion) {
        VersionNumber versionNumber = null;
        try {
            if (dseVersion != null) {
                versionNumber = VersionNumber.parse(dseVersion);
            }
        } catch (IllegalArgumentException e) {
            logger.warn("Error parsing DSE version {}. This shouldn't have happened", dseVersion);
        }
        this.dseVersion = versionNumber;
    }

    void setDseWorkload(String dseWorkload) {
        this.dseWorkload = dseWorkload;
    }

    void setDseGraphEnabled(boolean dseGraphEnabled) {
        this.dseGraphEnabled = dseGraphEnabled;
    }

    boolean supports(ProtocolVersion version) {
        return getCassandraVersion() == null
                || version.minCassandraVersion().compareTo(getCassandraVersion().nextStable()) <= 0;
    }

    /**
     * Returns the address that the driver will use to connect to the node.
     * <p/>
     * This is a shortcut for {@code getSocketAddress().getAddress()}.
     *
     * @return the address.
     * @see #getSocketAddress()
     */
    public InetAddress getAddress() {
        return address.getAddress();
    }

    /**
     * Returns the address and port that the driver will use to connect to the node.
     * <p/>
     * This is the node's broadcast RPC address, possibly translated if an {@link AddressTranslator} has been configured
     * for this cluster.
     * <p/>
     * The broadcast RPC address is inferred from the following cassandra.yaml file settings:
     * <ol>
     * <li>{@code rpc_address}, {@code rpc_interface} or {@code broadcast_rpc_address}</li>
     * <li>{@code native_transport_port}</li>
     * </ol>
     *
     * @return the address and port.
     * @see <a href="https://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html">The cassandra.yaml configuration file</a>
     */
    public InetSocketAddress getSocketAddress() {
        return address;
    }

    /**
     * Returns the node broadcast address (that is, the IP by which it should be contacted by other peers in the
     * cluster), if known.
     * <p/>
     * This corresponds to the {@code broadcast_address} cassandra.yaml file setting and
     * is by default the same as {@link #getListenAddress()}, unless specified
     * otherwise in cassandra.yaml.
     * <em>This is NOT the address clients should use to contact this node</em>.
     * <p/>
     * This information is always available for peer hosts. For the control host, it's only available if CASSANDRA-9436
     * is fixed on the server side (Cassandra versions >= 2.0.16, 2.1.6, 2.2.0 rc1). For older versions, note that if
     * the driver loses the control connection and reconnects to a different control host, the old control host becomes
     * a peer, and therefore its broadcast address is updated.
     *
     * @return the node broadcast address, if known. Otherwise {@code null}.
     * @see <a href="https://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html">The cassandra.yaml configuration file</a>
     */
    public InetAddress getBroadcastAddress() {
        return broadcastAddress;
    }

    /**
     * Returns the node listen address (that is, the IP the node uses to contact other peers in the cluster), if known.
     * <p/>
     * This corresponds to the {@code listen_address} cassandra.yaml file setting.
     * <em>This is NOT the address clients should use to contact this node</em>.
     * <p/>
     * This information is available for the control host if CASSANDRA-9603 is fixed on the server side (Cassandra
     * versions >= 2.0.17, 2.1.8, 2.2.0 rc2). It's currently not available for peer hosts. Note that the current driver
     * code already tries to read a {@code listen_address} column in {@code system.peers}; when a future Cassandra
     * version adds it, it will be picked by the driver without any further change needed.
     *
     * @return the node listen address, if known. Otherwise {@code null}.
     * @see <a href="https://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html">The cassandra.yaml configuration file</a>
     */
    public InetAddress getListenAddress() {
        return listenAddress;
    }

    /**
     * Returns the name of the datacenter this host is part of.
     * <p/>
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
     * <p/>
     * The returned rack name is the one as known by Cassandra.
     * It is also possible for this information to be unavailable. In that case
     * this method returns {@code null}, and the caller should always be aware of this
     * possibility.
     *
     * @return the Cassandra rack name or null if the rack is unavailable
     */
    public String getRack() {
        return rack;
    }

    /**
     * The Cassandra version the host is running.
     * <p/>
     * It is also possible for this information to be unavailable. In that case
     * this method returns {@code null}, and the caller should always be aware of this
     * possibility.
     *
     * @return the Cassandra version the host is running.
     */
    public VersionNumber getCassandraVersion() {
        return cassandraVersion;
    }

    /**
     * The DSE version the host is running.
     * <p/>
     * It is also possible for this information to be unavailable. In that case
     * this method returns {@code null}, and the caller should always be aware of this
     * possibility.
     *
     * @return the DSE version the host is running.
     * @deprecated Please use the
     * <a href="https://github.com/datastax/java-driver-dse">Java driver for DSE</a>
     * if you are connecting to a DataStax Enterprise (DSE) cluster.
     * This method might not function properly with future versions of DSE.
     */
    @Deprecated
    public VersionNumber getDseVersion() {
        return dseVersion;
    }

    /**
     * The DSE Workload the host is running.
     * <p/>
     * It is also possible for this information to be unavailable. In that case
     * this method returns {@code null}, and the caller should always be aware of this
     * possibility.
     *
     * @return the DSE workload the host is running.
     * @deprecated Please use the
     * <a href="https://github.com/datastax/java-driver-dse">Java driver for DSE</a>
     * if you are connecting to a DataStax Enterprise (DSE) cluster.
     * This method might not function properly with future versions of DSE.
     */
    @Deprecated
    public String getDseWorkload() {
        return dseWorkload;
    }

    /**
     * Returns whether the host is running DSE Graph.
     *
     * @return whether the node is running DSE Graph.
     * @deprecated Please use the
     * <a href="https://github.com/datastax/java-driver-dse">Java driver for DSE</a>
     * if you are connecting to a DataStax Enterprise (DSE) cluster.
     * This method might not function properly with future versions of DSE.
     */
    @Deprecated
    public boolean isDseGraphEnabled() {
        return dseGraphEnabled;
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
     * <p/>
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
        return state == State.UP;
    }

    /**
     * Returns a description of the host's state, as seen by the driver.
     * <p/>
     * This is exposed for debugging purposes only; the format of this string might
     * change between driver versions, so clients should not make any assumptions
     * about it.
     *
     * @return a description of the host's state.
     */
    public String getState() {
        return state.name();
    }

    /**
     * Returns a {@code ListenableFuture} representing the completion of the reconnection
     * attempts scheduled after a host is marked {@code DOWN}.
     * <p/>
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
     * <p/>
     * This method is intended for load balancing policies that mark hosts as {@link HostDistance#IGNORED IGNORED},
     * but still need a way to periodically check these hosts' states (UP / DOWN).
     * <p/>
     * For a host that is at distance {@code IGNORED}, this method will try to reconnect exactly once: if
     * reconnection succeeds, the host is marked {@code UP}; otherwise, no further attempts will be scheduled.
     * It has no effect if the node is already {@code UP}, or if a reconnection attempt is already in progress.
     * <p/>
     * Note that if the host is <em>not</em> a distance {@code IGNORED}, this method <em>will</em> trigger a periodic
     * reconnection attempt if the reconnection fails.
     */
    public void tryReconnectOnce() {
        this.manager.startSingleReconnectionAttempt(this);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Host) {
            Host that = (Host) other;
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
        state = State.UP;
    }

    /**
     * Interface for listeners that are interested in hosts added, up, down and
     * removed events.
     * <p/>
     * It is possible for the same event to be fired multiple times,
     * particularly for up or down events. Therefore, a listener should ignore
     * the same event if it has already been notified of a node's state.
     */
    public interface StateListener {

        /**
         * Called when a new node is added to the cluster.
         * <p/>
         * The newly added node should be considered up.
         *
         * @param host the host that has been newly added.
         */
        void onAdd(Host host);

        /**
         * Called when a node is determined to be up.
         *
         * @param host the host that has been detected up.
         */
        void onUp(Host host);

        /**
         * Called when a node is determined to be down.
         *
         * @param host the host that has been detected down.
         */
        void onDown(Host host);

        /**
         * Called when a node is removed from the cluster.
         *
         * @param host the removed host.
         */
        void onRemove(Host host);

        /**
         * Gets invoked when the tracker is registered with a cluster, or at cluster startup if the
         * tracker was registered at initialization with
         * {@link com.datastax.driver.core.Cluster.Initializer#register(LatencyTracker)}.
         *
         * @param cluster the cluster that this tracker is registered with.
         */
        void onRegister(Cluster cluster);

        /**
         * Gets invoked when the tracker is unregistered from a cluster, or at cluster shutdown if
         * the tracker was not unregistered.
         *
         * @param cluster the cluster that this tracker was registered with.
         */
        void onUnregister(Cluster cluster);
    }

    /**
     * A {@code StateListener} that tracks when it gets registered or unregistered with a cluster.
     * <p/>
     * This interface exists only for backward-compatibility reasons: starting with the 3.0 branch of the driver, its
     * methods are on the parent interface directly.
     */
    public interface LifecycleAwareStateListener extends StateListener {
        /**
         * Gets invoked when the listener is registered with a cluster, or at cluster startup if the
         * listener was registered at initialization with
         * {@link com.datastax.driver.core.Cluster#register(Host.StateListener)}.
         *
         * @param cluster the cluster that this listener is registered with.
         */
        @Override
        void onRegister(Cluster cluster);

        /**
         * Gets invoked when the listener is unregistered from a cluster, or at cluster shutdown if
         * the listener was not unregistered.
         *
         * @param cluster the cluster that this listener was registered with.
         */
        @Override
        void onUnregister(Cluster cluster);
    }
}
