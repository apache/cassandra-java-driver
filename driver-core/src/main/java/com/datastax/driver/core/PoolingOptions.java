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

import java.util.concurrent.Executor;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Options related to connection pooling.
 * <p>
 * The driver uses connections in an asynchronous manner. Meaning that
 * multiple requests can be submitted on the same connection at the same
 * time. This means that the driver only needs to maintain a relatively
 * small number of connections to each Cassandra host. These options allow
 * the driver to control how many connections are kept exactly.
 * <p>
 * <b>With {@code ProtocolVersion#V2} or below:</b>
 * for each host, the driver keeps a core pool of connections open at all
 * times determined by calling ({@link #getCoreConnectionsPerHost}).
 * If the use of those connections reaches a configurable threshold
 * ({@link #getMaxSimultaneousRequestsPerConnectionThreshold}),
 * more connections are created up to the configurable maximum number of
 * connections ({@link #getMaxConnectionsPerHost}). When the pool exceeds
 * the maximum number of connections, connections in excess are
 * reclaimed.
 * <p>
 * <b>With {@code ProtocolVersion#V3} or above:</b>
 * the driver uses a single connection for each {@code LOCAL} or {@code REMOTE}
 * host. This connection can handle a larger amount of simultaneous requests,
 * limited by {@link #getMaxSimultaneousRequestsPerHostThreshold(HostDistance)}.
 * <p>
 * Each of these parameters can be separately set for {@code LOCAL} and
 * {@code REMOTE} hosts ({@link HostDistance}). For {@code IGNORED} hosts,
 * the default for all those settings is 0 and cannot be changed.
 */
public class PoolingOptions {

    private static final int DEFAULT_MAX_REQUESTS_PER_CONNECTION = 100;

    private static final int DEFAULT_CORE_POOL_LOCAL = 2;
    private static final int DEFAULT_CORE_POOL_REMOTE = 1;

    private static final int DEFAULT_MAX_POOL_LOCAL = 8;
    private static final int DEFAULT_MAX_POOL_REMOTE = 2;

    private static final int DEFAULT_MAX_REQUESTS_PER_HOST_LOCAL = 1024;
    private static final int DEFAULT_MAX_REQUESTS_PER_HOST_REMOTE = 256;

    private static final int DEFAULT_IDLE_TIMEOUT_SECONDS = 120;
    private static final int DEFAULT_POOL_TIMEOUT_MILLIS = 5000;
    private static final int DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30;

    private static final Executor DEFAULT_INITIALIZATION_EXECUTOR = MoreExecutors.sameThreadExecutor();

    private volatile Cluster.Manager manager;

    private final int[] maxSimultaneousRequestsPerConnection = new int[]{ DEFAULT_MAX_REQUESTS_PER_CONNECTION, DEFAULT_MAX_REQUESTS_PER_CONNECTION, 0 };

    private final int[] coreConnections = new int[] { DEFAULT_CORE_POOL_LOCAL, DEFAULT_CORE_POOL_REMOTE, 0 };
    private final int[] maxConnections = new int[] { DEFAULT_MAX_POOL_LOCAL , DEFAULT_MAX_POOL_REMOTE, 0 };

    private volatile int maxSimultaneousRequestsPerHostLocal = DEFAULT_MAX_REQUESTS_PER_HOST_LOCAL;
    private volatile int maxSimultaneousRequestsPerHostRemote = DEFAULT_MAX_REQUESTS_PER_HOST_REMOTE;
    
    private volatile int idleTimeoutSeconds = DEFAULT_IDLE_TIMEOUT_SECONDS;
    private volatile int poolTimeoutMillis = DEFAULT_POOL_TIMEOUT_MILLIS;
    private volatile int heartbeatIntervalSeconds = DEFAULT_HEARTBEAT_INTERVAL_SECONDS;

    private volatile Executor initializationExecutor = DEFAULT_INITIALIZATION_EXECUTOR;

    public PoolingOptions() {}

    void register(Cluster.Manager manager) {
        this.manager = manager;
    }

    /**
     * Returns the number of simultaneous requests on a connection below which
     * connections in excess are reclaimed.
     * <p>
     * This option is only used with {@code ProtocolVersion#V2} or below.
     * <p>
     * If an opened connection to an host at distance {@code distance}
     * handles less than this number of simultaneous requests and there is
     * more than {@link #getCoreConnectionsPerHost} connections open to this
     * host, the connection is closed.
     * <p>
     * The default value for this option is 25 for {@code LOCAL} and
     * {@code REMOTE} hosts.
     *
     * @param distance the {@code HostDistance} for which to return this threshold.
     * @return the configured threshold, or the default one if none have been set.
     *
     * @deprecated this option isn't used anymore with the current pool resizing algorithm.
     */
    @Deprecated
    public int getMinSimultaneousRequestsPerConnectionThreshold(HostDistance distance) {
        return 0;
    }

    /**
     * Sets the number of simultaneous requests on a connection below which
     * connections in excess are reclaimed.
     * <p>
     * This option is only used with {@code ProtocolVersion#V2} or below.
     *
     * @param distance the {@code HostDistance} for which to configure this threshold.
     * @param newMinSimultaneousRequests the value to set (between 0 and 128).
     * @return this {@code PoolingOptions}.
     *
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED}, or if {@code minSimultaneousRequests}
     * is not in range, or if {@code newMinSimultaneousRequests} is greater than the maximum value for this distance.
     *
     * @deprecated this option isn't used anymore with the current pool resizing algorithm.
     */
    @Deprecated
    public synchronized PoolingOptions setMinSimultaneousRequestsPerConnectionThreshold(HostDistance distance, int newMinSimultaneousRequests) {
        return this;
    }

    /**
     * Returns the number of simultaneous requests on all connections to an host after
     * which more connections are created.
     * <p>
     * This option is only used with {@code ProtocolVersion#V2} or below.
     * <p>
     * If all the connections opened to an host at distance {@code
     * distance} connection are handling more than this number of
     * simultaneous requests and there is less than
     * {@link #getMaxConnectionsPerHost} connections open to this host, a
     * new connection is open.
     * <p>
     * Note that a given connection cannot handle more than 128
     * simultaneous requests (protocol limitation).
     * <p>
     * The default value for this option is 100 for {@code LOCAL} and
     * {@code REMOTE} hosts.
     *
     * @param distance the {@code HostDistance} for which to return this threshold.
     * @return the configured threshold, or the default one if none have been set.
     */
    public int getMaxSimultaneousRequestsPerConnectionThreshold(HostDistance distance) {
        return maxSimultaneousRequestsPerConnection[distance.ordinal()];
    }

    /**
     * Sets number of simultaneous requests on all connections to an host after
     * which more connections are created.
     * <p>
     * This option is only used with {@code ProtocolVersion#V2} or below.
     *
     * @param distance the {@code HostDistance} for which to configure this threshold.
     * @param newMaxSimultaneousRequests the value to set (between 0 and 128).
     * @return this {@code PoolingOptions}.
     *
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED}, or if {@code maxSimultaneousRequests}
     * is not in range, or if {@code newMaxSimultaneousRequests} is less than the minimum value for this distance.
     */
    public synchronized PoolingOptions setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance distance, int newMaxSimultaneousRequests) {
        if (distance == HostDistance.IGNORED)
            throw new IllegalArgumentException("Cannot set max simultaneous requests per connection threshold for " + distance + " hosts");

        checkRequestsPerConnectionRange(newMaxSimultaneousRequests, "Max simultaneous requests per connection", distance);
        maxSimultaneousRequestsPerConnection[distance.ordinal()] = newMaxSimultaneousRequests;
        return this;
    }

    /**
     * Returns the core number of connections per host.
     * <p>
     * This option is only used with {@code ProtocolVersion#V2} or below.
     * <p>
     * For the provided {@code distance}, this correspond to the number of
     * connections initially created and kept open to each host of that
     * distance.
     *
     * @param distance the {@code HostDistance} for which to return this threshold.
     * @return the core number of connections per host at distance {@code distance}.
     */
    public int getCoreConnectionsPerHost(HostDistance distance) {
        return coreConnections[distance.ordinal()];
    }

    /**
     * Sets the core number of connections per host.
     * <p>
     * This option is only used with {@code ProtocolVersion#V2} or below.
     *
     * @param distance the {@code HostDistance} for which to set this threshold.
     * @param newCoreConnections the value to set
     * @return this {@code PoolingOptions}.
     *
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED},
     * or if {@code newCoreConnections} is greater than the maximum value for this distance.
     */
    public synchronized PoolingOptions setCoreConnectionsPerHost(HostDistance distance, int newCoreConnections) {
        if (distance == HostDistance.IGNORED)
                throw new IllegalArgumentException("Cannot set core connections per host for " + distance + " hosts");

        checkConnectionsPerHostOrder(newCoreConnections, maxConnections[distance.ordinal()], distance);
        int oldCore = coreConnections[distance.ordinal()];
        coreConnections[distance.ordinal()] = newCoreConnections;
        if (oldCore < newCoreConnections && manager != null)
            manager.ensurePoolsSizing();
        return this;
    }

    /**
     * Returns the maximum number of connections per host.
     * <p>
     * This option is only used with {@code ProtocolVersion#V2} or below.
     * <p>
     * For the provided {@code distance}, this correspond to the maximum
     * number of connections that can be created per host at that distance.
     *
     * @param distance the {@code HostDistance} for which to return this threshold.
     * @return the maximum number of connections per host at distance {@code distance}.
     */
    public int getMaxConnectionsPerHost(HostDistance distance) {
        return maxConnections[distance.ordinal()];
    }

    /**
     * Sets the maximum number of connections per host.
     * <p>
     * This option is only used with {@code ProtocolVersion#V2} or below.
     *
     * @param distance the {@code HostDistance} for which to set this threshold.
     * @param newMaxConnections the value to set
     * @return this {@code PoolingOptions}.
     *
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED},
     * or if {@code newMaxConnections} is less than the core value for this distance.
     */
    public synchronized PoolingOptions setMaxConnectionsPerHost(HostDistance distance, int newMaxConnections) {
        if (distance == HostDistance.IGNORED)
            throw new IllegalArgumentException("Cannot set max connections per host for " + distance + " hosts");

        checkConnectionsPerHostOrder(coreConnections[distance.ordinal()], newMaxConnections, distance);
        maxConnections[distance.ordinal()] = newMaxConnections;
        return this;
    }

    /**
     * Sets the core and maximum number of connections per host in one call.
     * <p>
     * This is a convenience method that is equivalent to calling {@link #setCoreConnectionsPerHost(HostDistance, int)}
     * and {@link #setMaxConnectionsPerHost(HostDistance, int)}.
     *
     * @param distance the {@code HostDistance} for which to set these threshold.
     * @param core the core number of connections.
     * @param max the max number of connections.
     * @return this {@code PoolingOptions}.
     *
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED},
     * or if {@code core} > {@code max}.
     */
    public synchronized PoolingOptions setConnectionsPerHost(HostDistance distance, int core, int max) {
        if (distance == HostDistance.IGNORED)
            throw new IllegalArgumentException("Cannot set connections per host for " + distance + " hosts");

        checkConnectionsPerHostOrder(core, max, distance);
        coreConnections[distance.ordinal()] = core;
        maxConnections[distance.ordinal()] = max;
        return this;
    }

    /**
     * Returns the timeout before an idle connection is removed.
     *
     * @return the timeout.
     */
    public int getIdleTimeoutSeconds() {
        return idleTimeoutSeconds;
    }

    /**
     * Sets the timeout before an idle connection is removed.
     * <p>
     * The order of magnitude should be a few minutes (the default is 120 seconds). The
     * timeout that triggers the removal has a granularity of 10 seconds.
     *
     * @param idleTimeoutSeconds the new timeout in seconds.
     * @return this {@code PoolingOptions}.
     *
     * @throws IllegalArgumentException if the timeout is negative.
     */
    public PoolingOptions setIdleTimeoutSeconds(int idleTimeoutSeconds) {
        if (idleTimeoutSeconds < 0)
            throw new IllegalArgumentException("Idle timeout must be positive");
        this.idleTimeoutSeconds = idleTimeoutSeconds;
        return this;
    }

    /**
     * Returns the timeout when trying to acquire a connection from a host's pool.
     *
     * @return the timeout.
     */
    public int getPoolTimeoutMillis() {
        return poolTimeoutMillis;
    }

    /**
     * Sets the timeout when trying to acquire a connection from a host's pool.
     * <p>
     * If no connection is available within that time, the driver will try the
     * next host from the query plan.
     * <p>
     * If this option is set to zero, the driver won't wait at all.
     *
     * @param poolTimeoutMillis the new value in milliseconds.
     * @return this {@code PoolingOptions}
     *
     * @throws IllegalArgumentException if the timeout is negative.
     */
    public PoolingOptions setPoolTimeoutMillis(int poolTimeoutMillis) {
        if (poolTimeoutMillis < 0)
            throw new IllegalArgumentException("Pool timeout must be positive");
        this.poolTimeoutMillis = poolTimeoutMillis;
        return this;
    }

    /**
     * Returns the heart beat interval, after which a message is sent on an idle connection to make sure it's still alive.
     * @return the interval.
     */
    public int getHeartbeatIntervalSeconds() {
        return heartbeatIntervalSeconds;
    }

    /**
     * Sets the heart beat interval, after which a message is sent on an idle connection to make sure it's still alive.
     * <p>
     * This is an application-level keep-alive, provided for convenience since adjusting the TCP keep-alive might not be
     * practical in all environments.
     * <p>
     * This option should be set higher than {@link SocketOptions#getReadTimeoutMillis()}.
     * <p>
     * The default value for this option is 30 seconds.
     *
     * @param heartbeatIntervalSeconds the new value in seconds. If set to 0, it will disable the feature.
     * @return this {@code PoolingOptions}
     *
     * @throws IllegalArgumentException if the interval is negative.
     */
    public PoolingOptions setHeartbeatIntervalSeconds(int heartbeatIntervalSeconds) {
        if (heartbeatIntervalSeconds < 0)
            throw new IllegalArgumentException("Heartbeat interval must be positive");

        this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
        return this;
    }

    /**
     * Returns the maximum number of requests per host.
     * <p>
     * This option is only used with {@code ProtocolVersion#V3} or above.
     * <p>
     * The default value for this option is 1024 for {@code LOCAL} and 256 for
     * {@code REMOTE} hosts.
     *
     * @param distance the {@code HostDistance} for which to return this threshold.
     * @return the maximum number of requests per host at distance {@code distance}.
     */
    public int getMaxSimultaneousRequestsPerHostThreshold(HostDistance distance) {
        switch (distance) {
            case LOCAL:
                return maxSimultaneousRequestsPerHostLocal;
            case REMOTE:
                return maxSimultaneousRequestsPerHostRemote;
            default:
                return 0;
        }
    }

    /**
     * Sets the maximum number of requests per host.
     * <p>
     * This option is only used with {@code ProtocolVersion#V3} or above.
     *
     * @param distance the {@code HostDistance} for which to set this threshold.
     * @param newMaxRequests the value to set (between 1 and 32768).
     * @return this {@code PoolingOptions}.
     *
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED},
     * or if {@code newMaxConnections} is not within the allowed range.
     */
    public PoolingOptions setMaxSimultaneousRequestsPerHostThreshold(HostDistance distance, int newMaxRequests) {
        if (newMaxRequests <= 0 || newMaxRequests > StreamIdGenerator.MAX_STREAM_PER_CONNECTION_V3)
            throw new IllegalArgumentException(String.format("Max requests must be in the range (1, %d)",
                                               StreamIdGenerator.MAX_STREAM_PER_CONNECTION_V3));

        switch (distance) {
            case LOCAL:
                maxSimultaneousRequestsPerHostLocal = newMaxRequests;
                break;
            case REMOTE:
                maxSimultaneousRequestsPerHostRemote = newMaxRequests;
                break;
            default:
                throw new IllegalArgumentException("Cannot set max requests per host for " + distance + " hosts");
        }
        return this;
    }

    /**
     * Returns the executor to use for connection initialization.
     *
     * @return the executor.
     * @see #setInitializationExecutor(java.util.concurrent.Executor)
     */
    public Executor getInitializationExecutor() {
        return initializationExecutor;
    }

    /**
     * Sets the executor to use for connection initialization.
     * <p>
     * Connections are open in a completely asynchronous manner. Since initializing the transport
     * requires separate CQL queries, the futures representing the completion of these queries are
     * transformed and chained. This executor is where these transformations happen.
     * <p>
     * <b>This is an advanced option, which should be rarely needed in practice.</b> It defaults to
     * Guava's {@code MoreExecutors.sameThreadExecutor()}, which results in running the transformations
     * on the network I/O threads; this is fine if the transformations are fast and not I/O bound
     * (which is the case by default).
     * One reason why you might want to provide a custom executor is if you use authentication with
     * a custom {@link com.datastax.driver.core.Authenticator} implementation that performs blocking
     * calls.
     *
     * @param initializationExecutor the executor to use
     * @return this {@code PoolingOptions}
     *
     * @throws java.lang.NullPointerException if the executor is null
     */
    public PoolingOptions setInitializationExecutor(Executor initializationExecutor) {
        Preconditions.checkNotNull(initializationExecutor);
        this.initializationExecutor = initializationExecutor;
        return this;
    }

    /**
     * Requests the driver to re-evaluate the {@link HostDistance} (through the configured
     * {@link com.datastax.driver.core.policies.LoadBalancingPolicy#distance}) for every known
     * hosts and to drop/add connections to each hosts according to the computed distance.
     */
    public void refreshConnectedHosts() {
        manager.refreshConnectedHosts();
    }

    /**
     * Requests the driver to re-evaluate the {@link HostDistance} for a given node.
     *
     * @param host the host to refresh.
     *
     * @see #refreshConnectedHosts()
     */
    public void refreshConnectedHost(Host host) {
        manager.refreshConnectedHost(host);
    }

    private static void checkRequestsPerConnectionRange(int value, String description, HostDistance distance) {
        if (value < 0 || value > StreamIdGenerator.MAX_STREAM_PER_CONNECTION_V2)
            throw new IllegalArgumentException(String.format("%s for %s hosts must be in the range (0, %d)",
                                                             description, distance,
                                                             StreamIdGenerator.MAX_STREAM_PER_CONNECTION_V2));
    }

    private static void checkConnectionsPerHostOrder(int core, int max, HostDistance distance) {
        if (core > max)
            throw new IllegalArgumentException(String.format("Core connections for %s hosts must be less than max (%d > %d)",
                                                             distance, core, max));
    }
}
