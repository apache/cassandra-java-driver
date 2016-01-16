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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.Map;
import java.util.concurrent.Executor;

import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.HostDistance.REMOTE;

/**
 * Options related to connection pooling.
 * <p/>
 * The driver uses connections in an asynchronous manner, meaning that
 * multiple requests can be submitted on the same connection at the same
 * time. Therefore only a relatively small number of connections is needed.
 * For each host, the driver uses a connection pool that may have a variable
 * size (it will automatically adjust to the current load).
 * <p/>
 * With {@code ProtocolVersion#V2} or below, there are at most 128 simultaneous
 * requests per connection, so the pool defaults to a variable size. You will
 * typically raise the maximum capacity by adding more connections with
 * {@link #setMaxConnectionsPerHost(HostDistance, int)}.
 * <p/>
 * With {@code ProtocolVersion#V3} or above, there are up to 32768 requests per
 * connection, and the pool defaults to a fixed size of 1. You will typically
 * raise the maximum capacity by allowing more simultaneous requests per connection
 * ({@link #setMaxRequestsPerConnection(HostDistance, int)}).
 * <p/>
 * All parameters can be separately set for {@code LOCAL} and
 * {@code REMOTE} hosts ({@link HostDistance}). For {@code IGNORED} hosts,
 * no connections are created so these settings cannot be changed.
 */
public class PoolingOptions {

    /**
     * The value returned for connection options when they have not been set by the client, and the protocol version
     * is not known yet.
     * <p/>
     * Once a {@code PoolingOptions} object is associated to a {@link Cluster} and that cluster initializes, the
     * protocol version will be detected, and connection options will take their default values for that protocol
     * version.
     * <p/>
     * The methods that may return this value are:
     * {@link #getCoreConnectionsPerHost(HostDistance)},
     * {@link #getMaxConnectionsPerHost(HostDistance)},
     * {@link #getNewConnectionThreshold(HostDistance)},
     * {@link #getMaxRequestsPerConnection(HostDistance)}.
     */
    public static final int UNSET = Integer.MIN_VALUE;

    private static final Map<ProtocolVersion, Map<String, Integer>> DEFAULTS = ImmutableMap.<ProtocolVersion, Map<String, Integer>>of(
            ProtocolVersion.V2, ImmutableMap.<String, Integer>builder()
                    .put("corePoolLocal", 2)
                    .put("maxPoolLocal", 8)
                    .put("corePoolRemote", 1)
                    .put("maxPoolRemote", 2)
                    .put("newConnectionThresholdLocal", 100)
                    .put("newConnectionThresholdRemote", 100)
                    .put("maxRequestsPerConnectionLocal", 128)
                    .put("maxRequestsPerConnectionRemote", 128)
                    .build(),

            ProtocolVersion.V3, ImmutableMap.<String, Integer>builder()
                    .put("corePoolLocal", 1)
                    .put("maxPoolLocal", 1)
                    .put("corePoolRemote", 1)
                    .put("maxPoolRemote", 1)
                    .put("newConnectionThresholdLocal", 800)
                    .put("newConnectionThresholdRemote", 200)
                    .put("maxRequestsPerConnectionLocal", 1024)
                    .put("maxRequestsPerConnectionRemote", 256)
                    .build()
    );

    private static final int DEFAULT_IDLE_TIMEOUT_SECONDS = 120;
    private static final int DEFAULT_POOL_TIMEOUT_MILLIS = 5000;
    private static final int DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30;

    private static final Executor DEFAULT_INITIALIZATION_EXECUTOR = MoreExecutors.sameThreadExecutor();

    private volatile Cluster.Manager manager;
    private volatile ProtocolVersion protocolVersion;

    // The defaults for these fields depend on the protocol version, which is only known after control connection initialization.
    // Yet if the user set them before initialization, we want to keep their values. So we use -1 to mean "uninitialized".
    private final int[] coreConnections = new int[]{UNSET, UNSET, 0};
    private final int[] maxConnections = new int[]{UNSET, UNSET, 0};
    private final int[] newConnectionThreshold = new int[]{UNSET, UNSET, 0};
    private volatile int maxRequestsPerConnectionLocal = UNSET;
    private volatile int maxRequestsPerConnectionRemote = UNSET;

    private volatile int idleTimeoutSeconds = DEFAULT_IDLE_TIMEOUT_SECONDS;
    private volatile int poolTimeoutMillis = DEFAULT_POOL_TIMEOUT_MILLIS;
    private volatile int heartbeatIntervalSeconds = DEFAULT_HEARTBEAT_INTERVAL_SECONDS;

    private volatile Executor initializationExecutor = DEFAULT_INITIALIZATION_EXECUTOR;

    public PoolingOptions() {
    }

    void register(Cluster.Manager manager) {
        this.manager = manager;
    }

    /**
     * Returns the core number of connections per host.
     *
     * @param distance the {@code HostDistance} for which to return this threshold.
     * @return the core number of connections per host at distance {@code distance}.
     */
    public int getCoreConnectionsPerHost(HostDistance distance) {
        return coreConnections[distance.ordinal()];
    }

    /**
     * Sets the core number of connections per host.
     * <p/>
     * For the provided {@code distance}, this corresponds to the number of
     * connections initially created and kept open to each host of that
     * distance.
     * <p/>
     * The default value is:
     * <ul>
     * <li>with {@code ProtocolVersion#V2} or below: 2 for {@code LOCAL} hosts and 1 for {@code REMOTE} hosts.</li>
     * <li>with {@code ProtocolVersion#V3} or above: 1 for all hosts.</li>
     * </ul>
     *
     * @param distance           the {@code HostDistance} for which to set this threshold.
     * @param newCoreConnections the value to set
     * @return this {@code PoolingOptions}.
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED},
     *                                  or if {@code newCoreConnections} is greater than the maximum value for this distance.
     * @see #setConnectionsPerHost(HostDistance, int, int)
     */
    public synchronized PoolingOptions setCoreConnectionsPerHost(HostDistance distance, int newCoreConnections) {
        if (distance == HostDistance.IGNORED)
            throw new IllegalArgumentException("Cannot set core connections per host for " + distance + " hosts");
        Preconditions.checkArgument(newCoreConnections >= 0, "core number of connections must be positive");

        if (maxConnections[distance.ordinal()] != UNSET)
            checkConnectionsPerHostOrder(newCoreConnections, maxConnections[distance.ordinal()], distance);

        int oldCore = coreConnections[distance.ordinal()];
        coreConnections[distance.ordinal()] = newCoreConnections;
        if (oldCore < newCoreConnections && manager != null)
            manager.ensurePoolsSizing();
        return this;
    }

    /**
     * Returns the maximum number of connections per host.
     *
     * @param distance the {@code HostDistance} for which to return this threshold.
     * @return the maximum number of connections per host at distance {@code distance}.
     */
    public int getMaxConnectionsPerHost(HostDistance distance) {
        return maxConnections[distance.ordinal()];
    }

    /**
     * Sets the maximum number of connections per host.
     * <p/>
     * For the provided {@code distance}, this corresponds to the maximum
     * number of connections that can be created per host at that distance.
     * <p/>
     * The default value is:
     * <ul>
     * <li>with {@code ProtocolVersion#V2} or below: 8 for {@code LOCAL} hosts and 2 for {@code REMOTE} hosts.</li>
     * <li>with {@code ProtocolVersion#V3} or above: 1 for all hosts.</li>
     * </ul>
     *
     * @param distance          the {@code HostDistance} for which to set this threshold.
     * @param newMaxConnections the value to set
     * @return this {@code PoolingOptions}.
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED},
     *                                  or if {@code newMaxConnections} is less than the core value for this distance.
     * @see #setConnectionsPerHost(HostDistance, int, int)
     */
    public synchronized PoolingOptions setMaxConnectionsPerHost(HostDistance distance, int newMaxConnections) {
        if (distance == HostDistance.IGNORED)
            throw new IllegalArgumentException("Cannot set max connections per host for " + distance + " hosts");
        Preconditions.checkArgument(newMaxConnections >= 0, "max number of connections must be positive");

        if (coreConnections[distance.ordinal()] != UNSET)
            checkConnectionsPerHostOrder(coreConnections[distance.ordinal()], newMaxConnections, distance);

        maxConnections[distance.ordinal()] = newMaxConnections;
        return this;
    }

    /**
     * Sets the core and maximum number of connections per host in one call.
     * <p/>
     * This is a convenience method that is equivalent to calling {@link #setCoreConnectionsPerHost(HostDistance, int)}
     * and {@link #setMaxConnectionsPerHost(HostDistance, int)}.
     *
     * @param distance the {@code HostDistance} for which to set these threshold.
     * @param core     the core number of connections.
     * @param max      the max number of connections.
     * @return this {@code PoolingOptions}.
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED},
     *                                  or if {@code core} > {@code max}.
     */
    public synchronized PoolingOptions setConnectionsPerHost(HostDistance distance, int core, int max) {
        if (distance == HostDistance.IGNORED)
            throw new IllegalArgumentException("Cannot set connections per host for " + distance + " hosts");
        Preconditions.checkArgument(core >= 0, "core number of connections must be positive");
        Preconditions.checkArgument(max >= 0, "max number of connections must be positive");

        checkConnectionsPerHostOrder(core, max, distance);
        coreConnections[distance.ordinal()] = core;
        maxConnections[distance.ordinal()] = max;
        return this;
    }

    /**
     * Returns the threshold that triggers the creation of a new connection to a host.
     *
     * @param distance the {@code HostDistance} for which to return this threshold.
     * @return the configured threshold, or the default one if none have been set.
     * @see #setNewConnectionThreshold(HostDistance, int)
     */
    public int getNewConnectionThreshold(HostDistance distance) {
        return newConnectionThreshold[distance.ordinal()];
    }

    /**
     * Sets the threshold that triggers the creation of a new connection to a host.
     * <p/>
     * A new connection gets created if:
     * <ul>
     * <li>N connections are open</li>
     * <li><tt>N < {@link #getMaxConnectionsPerHost(HostDistance)}</tt></li>
     * <li>the number of active requests is more than
     * <tt>(N - 1) * {@link #getMaxRequestsPerConnection(HostDistance)} + {@link #getNewConnectionThreshold(HostDistance)}</tt>
     * </li>
     * </ul>
     * In other words, if all but the last connection are full, and the last connection is above this threshold.
     * <p/>
     * The default value is:
     * <ul>
     * <li>with {@code ProtocolVersion#V2} or below: 100 for all hosts.</li>
     * <li>with {@code ProtocolVersion#V3} or above: 800 for {@code LOCAL} hosts and 200 for {@code REMOTE} hosts.</li>
     * </ul>
     *
     * @param distance the {@code HostDistance} for which to configure this threshold.
     * @param newValue the value to set (between 0 and 128).
     * @return this {@code PoolingOptions}.
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED}, or if {@code maxSimultaneousRequests}
     *                                  is not in range, or if {@code newValue} is less than the minimum value for this distance.
     */
    public synchronized PoolingOptions setNewConnectionThreshold(HostDistance distance, int newValue) {
        if (distance == HostDistance.IGNORED)
            throw new IllegalArgumentException("Cannot set new connection threshold for " + distance + " hosts");

        checkRequestsPerConnectionRange(newValue, "New connection threshold", distance);
        newConnectionThreshold[distance.ordinal()] = newValue;
        return this;
    }

    /**
     * Returns the maximum number of requests per connection.
     *
     * @param distance the {@code HostDistance} for which to return this threshold.
     * @return the maximum number of requests per connection at distance {@code distance}.
     * @see #setMaxRequestsPerConnection(HostDistance, int)
     */
    public int getMaxRequestsPerConnection(HostDistance distance) {
        switch (distance) {
            case LOCAL:
                return maxRequestsPerConnectionLocal;
            case REMOTE:
                return maxRequestsPerConnectionRemote;
            default:
                return 0;
        }
    }

    /**
     * Sets the maximum number of requests per connection.
     * <p/>
     * The default value is:
     * <ul>
     * <li>with {@code ProtocolVersion#V2} or below: 128 for all hosts (there should not be any reason to change this).</li>
     * <li>with {@code ProtocolVersion#V3} or above: 1024 for {@code LOCAL} hosts and 256 for {@code REMOTE} hosts.
     * These values were chosen so that the default V2 and V3 configuration generate the same load on a Cassandra cluster.
     * Protocol V3 can go much higher (up to 32768), so if your number of clients is low, don't hesitate to experiment with
     * higher values. If you have more than one connection per host, consider also adjusting
     * {@link #setNewConnectionThreshold(HostDistance, int)}.
     * </li>
     * </ul>
     *
     * @param distance       the {@code HostDistance} for which to set this threshold.
     * @param newMaxRequests the value to set.
     * @return this {@code PoolingOptions}.
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED},
     *                                  or if {@code newMaxConnections} is not within the allowed range.
     */
    public PoolingOptions setMaxRequestsPerConnection(HostDistance distance, int newMaxRequests) {
        checkRequestsPerConnectionRange(newMaxRequests, "Max requests per connection", distance);

        switch (distance) {
            case LOCAL:
                maxRequestsPerConnectionLocal = newMaxRequests;
                break;
            case REMOTE:
                maxRequestsPerConnectionRemote = newMaxRequests;
                break;
            default:
                throw new IllegalArgumentException("Cannot set max requests per host for " + distance + " hosts");
        }
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
     * <p/>
     * The order of magnitude should be a few minutes (the default is 120 seconds). The
     * timeout that triggers the removal has a granularity of 10 seconds.
     *
     * @param idleTimeoutSeconds the new timeout in seconds.
     * @return this {@code PoolingOptions}.
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
     * <p/>
     * If no connection is available within that time, the driver will try the
     * next host from the query plan.
     * <p/>
     * The default is 5 seconds. If this option is set to zero, the driver won't wait at all.
     *
     * @param poolTimeoutMillis the new value in milliseconds.
     * @return this {@code PoolingOptions}
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
     *
     * @return the interval.
     */
    public int getHeartbeatIntervalSeconds() {
        return heartbeatIntervalSeconds;
    }

    /**
     * Sets the heart beat interval, after which a message is sent on an idle connection to make sure it's still alive.
     * <p/>
     * This is an application-level keep-alive, provided for convenience since adjusting the TCP keep-alive might not be
     * practical in all environments.
     * <p/>
     * This option should be set higher than {@link SocketOptions#getReadTimeoutMillis()}.
     * <p/>
     * The default value for this option is 30 seconds.
     *
     * @param heartbeatIntervalSeconds the new value in seconds. If set to 0, it will disable the feature.
     * @return this {@code PoolingOptions}
     * @throws IllegalArgumentException if the interval is negative.
     */
    public PoolingOptions setHeartbeatIntervalSeconds(int heartbeatIntervalSeconds) {
        if (heartbeatIntervalSeconds < 0)
            throw new IllegalArgumentException("Heartbeat interval must be positive");

        this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
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
     * <p/>
     * Connections are open in a completely asynchronous manner. Since initializing the transport
     * requires separate CQL queries, the futures representing the completion of these queries are
     * transformed and chained. This executor is where these transformations happen.
     * <p/>
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
     * @throws java.lang.NullPointerException if the executor is null
     */
    public PoolingOptions setInitializationExecutor(Executor initializationExecutor) {
        Preconditions.checkNotNull(initializationExecutor);
        this.initializationExecutor = initializationExecutor;
        return this;
    }

    synchronized void setProtocolVersion(ProtocolVersion protocolVersion) {
        this.protocolVersion = protocolVersion;

        ProtocolVersion referenceVersion = (protocolVersion.compareTo(ProtocolVersion.V2) <= 0)
                ? ProtocolVersion.V2
                : ProtocolVersion.V3;
        Map<String, Integer> defaults = DEFAULTS.get(referenceVersion);

        if (coreConnections[LOCAL.ordinal()] == UNSET)
            coreConnections[LOCAL.ordinal()] = defaults.get("corePoolLocal");
        if (maxConnections[LOCAL.ordinal()] == UNSET)
            maxConnections[LOCAL.ordinal()] = defaults.get("maxPoolLocal");
        checkConnectionsPerHostOrder(coreConnections[LOCAL.ordinal()], maxConnections[LOCAL.ordinal()], LOCAL);

        if (coreConnections[REMOTE.ordinal()] == UNSET)
            coreConnections[REMOTE.ordinal()] = defaults.get("corePoolRemote");
        if (maxConnections[REMOTE.ordinal()] == UNSET)
            maxConnections[REMOTE.ordinal()] = defaults.get("maxPoolRemote");
        checkConnectionsPerHostOrder(coreConnections[REMOTE.ordinal()], maxConnections[REMOTE.ordinal()], REMOTE);

        if (newConnectionThreshold[LOCAL.ordinal()] == UNSET)
            newConnectionThreshold[LOCAL.ordinal()] = defaults.get("newConnectionThresholdLocal");
        checkRequestsPerConnectionRange(newConnectionThreshold[LOCAL.ordinal()], "New connection threshold", LOCAL);

        if (newConnectionThreshold[REMOTE.ordinal()] == UNSET)
            newConnectionThreshold[REMOTE.ordinal()] = defaults.get("newConnectionThresholdRemote");
        checkRequestsPerConnectionRange(newConnectionThreshold[REMOTE.ordinal()], "New connection threshold", REMOTE);

        if (maxRequestsPerConnectionLocal == UNSET)
            maxRequestsPerConnectionLocal = defaults.get("maxRequestsPerConnectionLocal");
        checkRequestsPerConnectionRange(maxRequestsPerConnectionLocal, "Max requests per connection", LOCAL);

        if (maxRequestsPerConnectionRemote == UNSET)
            maxRequestsPerConnectionRemote = defaults.get("maxRequestsPerConnectionRemote");
        checkRequestsPerConnectionRange(maxRequestsPerConnectionRemote, "Max requests per connection", REMOTE);
    }

    /**
     * Requests the driver to re-evaluate the {@link HostDistance} (through the configured
     * {@link com.datastax.driver.core.policies.LoadBalancingPolicy#distance}) for every known
     * hosts and to drop/add connections to each hosts according to the computed distance.
     * <p/>
     * Note that, due to backward compatibility issues, this method is not interruptible. If the
     * caller thread gets interrupted, the method will complete and only then re-interrupt the
     * thread (which you can check with {@code Thread.currentThread().isInterrupted()}).
     */
    public void refreshConnectedHosts() {
        manager.refreshConnectedHosts();
    }

    /**
     * Requests the driver to re-evaluate the {@link HostDistance} for a given node.
     *
     * @param host the host to refresh.
     * @see #refreshConnectedHosts()
     */
    public void refreshConnectedHost(Host host) {
        manager.refreshConnectedHost(host);
    }

    private void checkRequestsPerConnectionRange(int value, String description, HostDistance distance) {
        // If we don't know the protocol version yet, use the highest possible upper bound, this will get checked again when possible
        int max = (protocolVersion == null || protocolVersion.compareTo(ProtocolVersion.V3) >= 0)
                ? StreamIdGenerator.MAX_STREAM_PER_CONNECTION_V3
                : StreamIdGenerator.MAX_STREAM_PER_CONNECTION_V2;

        if (value < 0 || value > max)
            throw new IllegalArgumentException(String.format("%s for %s hosts must be in the range (0, %d)",
                    description, distance, max));
    }

    private static void checkConnectionsPerHostOrder(int core, int max, HostDistance distance) {
        if (core > max)
            throw new IllegalArgumentException(String.format("Core connections for %s hosts must be less than max (%d > %d)",
                    distance, core, max));
    }

}
