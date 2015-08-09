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

import com.datastax.driver.core.exceptions.UnsupportedFeatureException;

/**
 * Options related to defaults for individual queries.
 */
public class QueryOptions {

    /**
     * The default consistency level for queries: {@link ConsistencyLevel#ONE}.
     */
    public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.ONE;

    /**
     * The default serial consistency level for conditional updates: {@link ConsistencyLevel#SERIAL}.
     */
    public static final ConsistencyLevel DEFAULT_SERIAL_CONSISTENCY_LEVEL = ConsistencyLevel.SERIAL;

    /**
     * The default fetch size for SELECT queries: 5000.
     */
    public static final int DEFAULT_FETCH_SIZE = 5000;

    /**
     * The default value for {@link #getDefaultIdempotence()}: {@code false}.
     */
    public static final boolean DEFAULT_IDEMPOTENCE = false;

    private volatile ConsistencyLevel consistency = DEFAULT_CONSISTENCY_LEVEL;
    private volatile ConsistencyLevel serialConsistency = DEFAULT_SERIAL_CONSISTENCY_LEVEL;
    private volatile int fetchSize = DEFAULT_FETCH_SIZE;
    private volatile boolean defaultIdempotence = DEFAULT_IDEMPOTENCE;
    private volatile boolean reprepareOnUp = true;
    private volatile Cluster.Manager manager;
    private volatile boolean prepareOnAllHosts = true;

    /**
     * Creates a new {@link QueryOptions} instance using the {@link #DEFAULT_CONSISTENCY_LEVEL},
     * {@link #DEFAULT_SERIAL_CONSISTENCY_LEVEL} and {@link #DEFAULT_FETCH_SIZE}.
     */
    public QueryOptions() {}

    void register(Cluster.Manager manager) {
        this.manager = manager;
    }

    /**
     * Sets the default consistency level to use for queries.
     * <p>
     * The consistency level set through this method will be use for queries
     * that don't explicitly have a consistency level, i.e. when {@link Statement#getConsistencyLevel}
     * returns {@code null}.
     *
     * @param consistencyLevel the new consistency level to set as default.
     * @return this {@code QueryOptions} instance.
     */
    public QueryOptions setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistency = consistencyLevel;
        return this;
    }

    /**
     * The default consistency level used by queries.
     *
     * @return the default consistency level used by queries.
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistency;
    }

    /**
     * Sets the default serial consistency level to use for queries.
     * <p>
     * The serial consistency level set through this method will be use for queries
     * that don't explicitly have a serial consistency level, i.e. when {@link Statement#getSerialConsistencyLevel}
     * returns {@code null}.
     *
     * @param serialConsistencyLevel the new serial consistency level to set as default.
     * @return this {@code QueryOptions} instance.
     */
    public QueryOptions setSerialConsistencyLevel(ConsistencyLevel serialConsistencyLevel) {
        this.serialConsistency = serialConsistencyLevel;
        return this;
    }

    /**
     * The default serial consistency level used by queries.
     *
     * @return the default serial consistency level used by queries.
     */
    public ConsistencyLevel getSerialConsistencyLevel() {
        return serialConsistency;
    }

    /**
     * Sets the default fetch size to use for SELECT queries.
     * <p>
     * The fetch size set through this method will be use for queries
     * that don't explicitly have a fetch size, i.e. when {@link Statement#getFetchSize}
     * is less or equal to 0.
     *
     * @param fetchSize the new fetch size to set as default. It must be
     * strictly positive but you can use {@code Integer.MAX_VALUE} to disable
     * paging.
     * @return this {@code QueryOptions} instance.
     *
     * @throws IllegalArgumentException if {@code fetchSize &lte; 0}.
     * @throws UnsupportedFeatureException if version 1 of the native protocol is in
     * use and {@code fetchSize != Integer.MAX_VALUE} as paging is not supported by
     * version 1 of the protocol. See {@link Cluster.Builder#withProtocolVersion}
     * for more details on protocol versions.
     */
    public QueryOptions setFetchSize(int fetchSize) {
        if (fetchSize <= 0)
            throw new IllegalArgumentException("Invalid fetchSize, should be > 0, got " + fetchSize);

        int version = manager == null ? -1 : manager.protocolVersion();
        if (fetchSize != Integer.MAX_VALUE && version == 1)
            throw new UnsupportedFeatureException("Paging is not supported");

        this.fetchSize = fetchSize;
        return this;
    }

    /**
     * The default fetch size used by queries.
     *
     * @return the default fetch size used by queries.
     */
    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * Sets the default idempotence for queries.
     * <p>
     * This will be used for statements for which {@link com.datastax.driver.core.Statement#isIdempotent()}
     * returns {@code null}.
     *
     * @param defaultIdempotence the new value to set as default idempotence.
     * @return this {@code QueryOptions} instance.
     */
    public QueryOptions setDefaultIdempotence(boolean defaultIdempotence) {
        this.defaultIdempotence = defaultIdempotence;
        return this;
    }

    /**
     * The default idempotence for queries.
     * <p>
     * It defaults to {@link #DEFAULT_IDEMPOTENCE}.
     *
     * @return the default idempotence for queries.
     */
    public boolean getDefaultIdempotence() {
        return defaultIdempotence;
    }

    /**
     * Set whether the driver should prepare statements on all hosts in the cluster.
     * <p>
     * A statement is normally prepared in two steps:
     * <ol>
     *     <li>prepare the query on a single host in the cluster;</li>
     *     <li>if that succeeds, prepare on all other hosts.</li>
     * </ol>
     * This option controls whether step 2 is executed. It is enabled by default.
     * <p>
     * The reason why you might want to disable it is to optimize network usage if you
     * have a large number of clients preparing the same set of statements at startup.
     * If your load balancing policy distributes queries randomly, each client will pick
     * a different host to prepare its statements, and on the whole each host has a good
     * chance of having been hit by at least one client for each statement.
     * <p>
     * On the other hand, if that assumption turns out to be wrong and one host hasn't
     * prepared a given statement, it needs to be re-prepared on the fly the first time
     * it gets executed; this causes a performance penalty (one extra roundtrip to resend
     * the query to prepare, and another to retry the execution).
     *
     * @param prepareOnAllHosts the new value to set to indicate whether to prepare
     *                          statements once or on all nodes.
     * @return this {@code QueryOptions} instance.
     */
    public QueryOptions setPrepareOnAllHosts(boolean prepareOnAllHosts) {
        this.prepareOnAllHosts = prepareOnAllHosts;
        return this;
    }

    /**
     * Returns whether the driver should prepare statements on all hosts in the cluster.
     *
     * @return the value.
     *
     * @see #setPrepareOnAllHosts(boolean)
     */
    public boolean isPrepareOnAllHosts() {
        return this.prepareOnAllHosts;
    }

    /**
     * Set whether the driver should re-prepare all cached prepared statements on a host
     * when it marks it back up.
     * <p>
     * This option is enabled by default.
     * <p>
     * The reason why you might want to disable it is to optimize reconnection time when
     * you believe hosts often get marked down because of temporary network issues, rather
     * than the host really crashing. In that case, the host still has prepared statements
     * in its cache when the driver reconnects, so re-preparing is redundant.
     * <p>
     * On the other hand, if that assumption turns out to be wrong and the host had
     * really restarted, its prepared statement cache is empty, and statements need to be
     * re-prepared on the fly the first time they get executed; this causes a performance
     * penalty (one extra roundtrip to resend the query to prepare, and another to retry
     * the execution).
     *
     * @param reprepareOnUp whether the driver should re-prepare when marking a node up.
     * @return this {@code QueryOptions} instance.
     */
    public QueryOptions setReprepareOnUp(boolean reprepareOnUp){
        this.reprepareOnUp = reprepareOnUp;
        return this;
    }

    /**
     * Whether the driver should re-prepare all cached prepared statements on a host
     * when its marks that host back up.
     *
     * @return the value.
     *
     * @see #setReprepareOnUp(boolean)
     */
    public boolean isReprepareOnUp() {
        return this.reprepareOnUp;
    }
}
