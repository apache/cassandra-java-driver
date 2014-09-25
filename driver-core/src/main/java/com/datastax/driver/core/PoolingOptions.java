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

/**
 * Options related to connection pooling.
 * <p>
 * The driver uses connections in an asynchronous manner. Meaning that
 * multiple requests can be submitted on the same connection at the same
 * time. This means that the driver only needs to maintain a relatively
 * small number of connections to each Cassandra host. These options allow
 * the driver to control how many connections are kept exactly.
 * <p>
 * For each host, the driver keeps a core pool of connections open at all
 * times determined by calling ({@link #getCoreConnectionsPerHost}).
 * If the use of those connections reaches a configurable threshold
 * ({@link #getMaxSimultaneousRequestsPerConnectionThreshold}),
 * more connections are created up to the configurable maximum number of
 * connections ({@link #getMaxConnectionsPerHost}). When the pool exceeds
 * the maximum number of connections, connections in excess are
 * reclaimed if the use of opened connections drops below the
 * configured threshold ({@link #getMinSimultaneousRequestsPerConnectionThreshold}).
 * <p>
 * Each of these parameters can be separately set for {@code LOCAL} and
 * {@code REMOTE} hosts ({@link HostDistance}). For {@code IGNORED} hosts,
 * the default for all those settings is 0 and cannot be changed.
 */
public class PoolingOptions {

    // Note: we could use an enumMap or similar, but synchronization would
    // be more costly so let's stick to volatile in for now.
    private static final int DEFAULT_MIN_REQUESTS = 25;
    private static final int DEFAULT_MAX_REQUESTS = 100;

    private static final int DEFAULT_CORE_POOL_LOCAL = 2;
    private static final int DEFAULT_CORE_POOL_REMOTE = 1;

    private static final int DEFAULT_MAX_POOL_LOCAL = 8;
    private static final int DEFAULT_MAX_POOL_REMOTE = 2;

    private static final int MAX_STREAM_PER_CONNECTION_V2 = 128;

    private volatile Cluster.Manager manager;

    private final int[] minSimultaneousRequests = new int[]{ DEFAULT_MIN_REQUESTS, DEFAULT_MIN_REQUESTS, 0 };
    private final int[] maxSimultaneousRequests = new int[]{ DEFAULT_MAX_REQUESTS, DEFAULT_MAX_REQUESTS, 0 };

    private final int[] coreConnections = new int[] { DEFAULT_CORE_POOL_LOCAL, DEFAULT_CORE_POOL_REMOTE, 0 };
    private final int[] maxConnections = new int[] { DEFAULT_MAX_POOL_LOCAL , DEFAULT_MAX_POOL_REMOTE, 0 };

    public PoolingOptions() {}

    void register(Cluster.Manager manager) {
        this.manager = manager;
    }

    /**
     * Returns the number of simultaneous requests on a connection below which
     * connections in excess are reclaimed.
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
     */
    public int getMinSimultaneousRequestsPerConnectionThreshold(HostDistance distance) {
        return minSimultaneousRequests[distance.ordinal()];
    }

    /**
     * Sets the number of simultaneous requests on a connection below which
     * connections in excess are reclaimed.
     *
     * @param distance the {@code HostDistance} for which to configure this threshold.
     * @param newMinSimultaneousRequests the value to set (between 0 and 128).
     * @return this {@code PoolingOptions}.
     *
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED}, or if {@code minSimultaneousRequests}
     * is not in range, or if {@code newMinSimultaneousRequests} is greater than the maximum value for this distance.
     */
    public synchronized PoolingOptions setMinSimultaneousRequestsPerConnectionThreshold(HostDistance distance, int newMinSimultaneousRequests) {
        if (distance == HostDistance.IGNORED)
            throw new IllegalArgumentException("Cannot set min simultaneous requests per connection threshold for " + distance + " hosts");

        checkRequestsPerConnectionRange(newMinSimultaneousRequests, "Min simultaneous requests per connection", distance);
        checkRequestsPerConnectionOrder(newMinSimultaneousRequests, maxSimultaneousRequests[distance.ordinal()], distance);
        minSimultaneousRequests[distance.ordinal()] = newMinSimultaneousRequests;
        return this;
    }

    /**
     * Returns the number of simultaneous requests on all connections to an host after
     * which more connections are created.
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
        return maxSimultaneousRequests[distance.ordinal()];
    }

    /**
     * Sets number of simultaneous requests on all connections to an host after
     * which more connections are created.
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
        checkRequestsPerConnectionOrder(minSimultaneousRequests[distance.ordinal()], newMaxSimultaneousRequests, distance);
        maxSimultaneousRequests[distance.ordinal()] = newMaxSimultaneousRequests;
        return this;
    }

    /**
     * Returns the core number of connections per host.
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
     * Requests the driver to re-evaluate the {@link HostDistance} (through the configured
     * {@link com.datastax.driver.core.policies.LoadBalancingPolicy#distance}) for every known
     * hosts and to drop/add connections to each hosts according to the computed distance.
     */
    public void refreshConnectedHosts() {
        manager.refreshConnectedHosts();
    }

    private static void checkRequestsPerConnectionRange(int value, String description, HostDistance distance) {
        if (value < 0 || value > MAX_STREAM_PER_CONNECTION_V2)
            throw new IllegalArgumentException(String.format("%s for %s hosts must be in the range (0, %d)",
                                                             description, distance,
                                                             MAX_STREAM_PER_CONNECTION_V2));
    }

    private static void checkRequestsPerConnectionOrder(int min, int max, HostDistance distance) {
        if (min > max)
            throw new IllegalArgumentException(String.format("Min simultaneous requests per connection for %s hosts must be less than max (%d > %d)",
                                                             distance, min, max));
    }

    private static void checkConnectionsPerHostOrder(int core, int max, HostDistance distance) {
        if (core > max)
            throw new IllegalArgumentException(String.format("Core connections for %s hosts must be less than max (%d > %d)",
                                                             distance, core, max));
    }
}
