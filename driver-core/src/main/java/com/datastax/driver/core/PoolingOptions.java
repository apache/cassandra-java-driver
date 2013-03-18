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
 * ({@link #getMaxSimultaneousRequestsPerConnectionTreshold}),
 * more connections are created up to the configurable maximum number of
 * connections ({@link #getMaxConnectionPerHost}). When the pool exceeds
 * the maximum number of connections, connections in excess are
 * reclaimed if the use of opened connections drops below the
 * configured threshold ({@link #getMinSimultaneousRequestsPerConnectionTreshold}).
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

    private volatile Cluster.Manager manager;

    private volatile int minSimultaneousRequestsForLocal = DEFAULT_MIN_REQUESTS;
    private volatile int minSimultaneousRequestsForRemote = DEFAULT_MIN_REQUESTS;

    private volatile int maxSimultaneousRequestsForLocal = DEFAULT_MAX_REQUESTS;
    private volatile int maxSimultaneousRequestsForRemote = DEFAULT_MAX_REQUESTS;

    private volatile int coreConnectionsForLocal = DEFAULT_CORE_POOL_LOCAL;
    private volatile int coreConnectionsForRemote = DEFAULT_CORE_POOL_REMOTE;

    private volatile int maxConnectionsForLocal = DEFAULT_MAX_POOL_LOCAL;
    private volatile int maxConnectionsForRemote = DEFAULT_MAX_POOL_REMOTE;

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
    public int getMinSimultaneousRequestsPerConnectionTreshold(HostDistance distance) {
        switch (distance) {
            case LOCAL:
                return minSimultaneousRequestsForLocal;
            case REMOTE:
                return minSimultaneousRequestsForRemote;
            default:
                return 0;
        }
    }

    /**
     * Sets the number of simultaneous requests on a connection below which
     * connections in excess are reclaimed.
     *
     * @param distance the {@code HostDistance} for which to configure this threshold.
     * @param minSimultaneousRequests the value to set.
     * @return this {@code PoolingOptions}.
     *
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED}.
     */
    public PoolingOptions setMinSimultaneousRequestsPerConnectionTreshold(HostDistance distance, int minSimultaneousRequests) {
        switch (distance) {
            case LOCAL:
                minSimultaneousRequestsForLocal = minSimultaneousRequests;
                break;
            case REMOTE:
                minSimultaneousRequestsForRemote = minSimultaneousRequests;
                break;
            default:
                throw new IllegalArgumentException("Cannot set min streams per connection threshold for " + distance + " hosts");
        }
        return this;
    }

    /**
     * Returns the number of simultaneous requests on all connections to an host after
     * which more connections are created.
     * <p>
     * If all the connections opened to an host at distance {@code
     * distance} connection are handling more than this number of
     * simultaneous requests and there is less than
     * {@link #getMaxConnectionPerHost} connections open to this host, a
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
    public int getMaxSimultaneousRequestsPerConnectionTreshold(HostDistance distance) {
        switch (distance) {
            case LOCAL:
                return maxSimultaneousRequestsForLocal;
            case REMOTE:
                return maxSimultaneousRequestsForRemote;
            default:
                return 0;
        }
    }

    /**
     * Sets number of simultaneous requests on all connections to an host after
     * which more connections are created.
     *
     * @param distance the {@code HostDistance} for which to configure this threshold.
     * @param maxSimultaneousRequests the value to set.
     * @return this {@code PoolingOptions}.
     *
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED}.
     */
    public PoolingOptions setMaxSimultaneousRequestsPerConnectionTreshold(HostDistance distance, int maxSimultaneousRequests) {
        switch (distance) {
            case LOCAL:
                maxSimultaneousRequestsForLocal = maxSimultaneousRequests;
                break;
            case REMOTE:
                maxSimultaneousRequestsForRemote = maxSimultaneousRequests;
                break;
            default:
                throw new IllegalArgumentException("Cannot set max streams per connection threshold for " + distance + " hosts");
        }
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
        switch (distance) {
            case LOCAL:
                return coreConnectionsForLocal;
            case REMOTE:
                return coreConnectionsForRemote;
            default:
                return 0;
        }
    }

    /**
     * Sets the core number of connections per host.
     *
     * @param distance the {@code HostDistance} for which to set this threshold.
     * @param coreConnections the value to set
     * @return this {@code PoolingOptions}.
     *
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED}.
     */
    public PoolingOptions setCoreConnectionsPerHost(HostDistance distance, int coreConnections) {
        switch (distance) {
            case LOCAL:
                int oldLocalCore = coreConnectionsForLocal;
                coreConnectionsForLocal = coreConnections;
                if (oldLocalCore < coreConnectionsForLocal && manager != null)
                    manager.ensurePoolsSizing();
                break;
            case REMOTE:
                int oldRemoteCore = coreConnectionsForRemote;
                coreConnectionsForRemote = coreConnections;
                if (oldRemoteCore < coreConnectionsForRemote && manager != null)
                    manager.ensurePoolsSizing();
                break;
            default:
                throw new IllegalArgumentException("Cannot set core connections per host for " + distance + " hosts");
        }
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
    public int getMaxConnectionPerHost(HostDistance distance) {
        switch (distance) {
            case LOCAL:
                return maxConnectionsForLocal;
            case REMOTE:
                return maxConnectionsForRemote;
            default:
                return 0;
        }
    }

    /**
     * Sets the maximum number of connections per host.
     *
     * @param distance the {@code HostDistance} for which to set this threshold.
     * @param maxConnections the value to set
     * @return this {@code PoolingOptions}.
     *
     * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED}.
     */
    public PoolingOptions setMaxConnectionsPerHost(HostDistance distance, int maxConnections) {
        switch (distance) {
            case LOCAL:
                maxConnectionsForLocal = maxConnections;
                break;
            case REMOTE:
                maxConnectionsForRemote = maxConnections;
                break;
            default:
                throw new IllegalArgumentException("Cannot set max connections per host for " + distance + " hosts");
        }
        return this;
    }
}
