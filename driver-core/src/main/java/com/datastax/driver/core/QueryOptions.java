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
     * The default consistency level for queries: {@link ConsistencyLevel#LOCAL_ONE}.
     */
    public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_ONE;

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

    public static final int DEFAULT_MAX_PENDING_REFRESH_NODE_LIST_REQUESTS = 20;

    public static final int DEFAULT_MAX_PENDING_REFRESH_NODE_REQUESTS = 20;

    public static final int DEFAULT_MAX_PENDING_REFRESH_SCHEMA_REQUESTS = 20;

    public static final int DEFAULT_REFRESH_NODE_LIST_INTERVAL_MILLIS = 1000;

    public static final int DEFAULT_REFRESH_NODE_INTERVAL_MILLIS = 1000;

    public static final int DEFAULT_REFRESH_SCHEMA_INTERVAL_MILLIS = 1000;

    private volatile ConsistencyLevel consistency = DEFAULT_CONSISTENCY_LEVEL;
    private volatile ConsistencyLevel serialConsistency = DEFAULT_SERIAL_CONSISTENCY_LEVEL;
    private volatile int fetchSize = DEFAULT_FETCH_SIZE;
    private volatile boolean defaultIdempotence = DEFAULT_IDEMPOTENCE;

    private volatile boolean metadataEnabled = true;

    private volatile int maxPendingRefreshNodeListRequests = DEFAULT_MAX_PENDING_REFRESH_NODE_LIST_REQUESTS;
    private volatile int maxPendingRefreshNodeRequests = DEFAULT_MAX_PENDING_REFRESH_NODE_REQUESTS;
    private volatile int maxPendingRefreshSchemaRequests = DEFAULT_MAX_PENDING_REFRESH_SCHEMA_REQUESTS;

    private volatile int refreshNodeListIntervalMillis = DEFAULT_REFRESH_NODE_LIST_INTERVAL_MILLIS;
    private volatile int refreshNodeIntervalMillis = DEFAULT_REFRESH_NODE_INTERVAL_MILLIS;
    private volatile int refreshSchemaIntervalMillis = DEFAULT_REFRESH_SCHEMA_INTERVAL_MILLIS;

    private volatile boolean reprepareOnUp = true;
    private volatile Cluster.Manager manager;
    private volatile boolean prepareOnAllHosts = true;

    /**
     * Creates a new {@link QueryOptions} instance using the {@link #DEFAULT_CONSISTENCY_LEVEL},
     * {@link #DEFAULT_SERIAL_CONSISTENCY_LEVEL} and {@link #DEFAULT_FETCH_SIZE}.
     */
    public QueryOptions() {
    }

    void register(Cluster.Manager manager) {
        this.manager = manager;
    }

    /**
     * Sets the default consistency level to use for queries.
     * <p/>
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
     * <p/>
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
     * <p/>
     * The fetch size set through this method will be use for queries
     * that don't explicitly have a fetch size, i.e. when {@link Statement#getFetchSize}
     * is less or equal to 0.
     *
     * @param fetchSize the new fetch size to set as default. It must be
     *                  strictly positive but you can use {@code Integer.MAX_VALUE} to disable
     *                  paging.
     * @return this {@code QueryOptions} instance.
     * @throws IllegalArgumentException    if {@code fetchSize &lte; 0}.
     * @throws UnsupportedFeatureException if version 1 of the native protocol is in
     *                                     use and {@code fetchSize != Integer.MAX_VALUE} as paging is not supported by
     *                                     version 1 of the protocol. See {@link Cluster.Builder#withProtocolVersion}
     *                                     for more details on protocol versions.
     */
    public QueryOptions setFetchSize(int fetchSize) {
        if (fetchSize <= 0)
            throw new IllegalArgumentException("Invalid fetchSize, should be > 0, got " + fetchSize);

        ProtocolVersion version = manager == null ? null : manager.protocolVersion();
        if (fetchSize != Integer.MAX_VALUE && version == ProtocolVersion.V1)
            throw new UnsupportedFeatureException(version, "Paging is not supported");

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
     * <p/>
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
     * <p/>
     * It defaults to {@link #DEFAULT_IDEMPOTENCE}.
     *
     * @return the default idempotence for queries.
     */
    public boolean getDefaultIdempotence() {
        return defaultIdempotence;
    }

    /**
     * Set whether the driver should prepare statements on all hosts in the cluster.
     * <p/>
     * A statement is normally prepared in two steps:
     * <ol>
     * <li>prepare the query on a single host in the cluster;</li>
     * <li>if that succeeds, prepare on all other hosts.</li>
     * </ol>
     * This option controls whether step 2 is executed. It is enabled by default.
     * <p/>
     * The reason why you might want to disable it is to optimize network usage if you
     * have a large number of clients preparing the same set of statements at startup.
     * If your load balancing policy distributes queries randomly, each client will pick
     * a different host to prepare its statements, and on the whole each host has a good
     * chance of having been hit by at least one client for each statement.
     * <p/>
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
     * @see #setPrepareOnAllHosts(boolean)
     */
    public boolean isPrepareOnAllHosts() {
        return this.prepareOnAllHosts;
    }

    /**
     * Set whether the driver should re-prepare all cached prepared statements on a host
     * when it marks it back up.
     * <p/>
     * This option is enabled by default.
     * <p/>
     * The reason why you might want to disable it is to optimize reconnection time when
     * you believe hosts often get marked down because of temporary network issues, rather
     * than the host really crashing. In that case, the host still has prepared statements
     * in its cache when the driver reconnects, so re-preparing is redundant.
     * <p/>
     * On the other hand, if that assumption turns out to be wrong and the host had
     * really restarted, its prepared statement cache is empty, and statements need to be
     * re-prepared on the fly the first time they get executed; this causes a performance
     * penalty (one extra roundtrip to resend the query to prepare, and another to retry
     * the execution).
     *
     * @param reprepareOnUp whether the driver should re-prepare when marking a node up.
     * @return this {@code QueryOptions} instance.
     */
    public QueryOptions setReprepareOnUp(boolean reprepareOnUp) {
        this.reprepareOnUp = reprepareOnUp;
        return this;
    }

    /**
     * Whether the driver should re-prepare all cached prepared statements on a host
     * when its marks that host back up.
     *
     * @return the value.
     * @see #setReprepareOnUp(boolean)
     */
    public boolean isReprepareOnUp() {
        return this.reprepareOnUp;
    }

    /**
     * Toggle client-side token and schema metadata.
     * <p/>
     * This feature is enabled by default. Some applications might wish to disable it
     * in order to eliminate the overhead of querying the metadata and building its
     * client-side representation. However, take note that doing so will have important
     * consequences:
     * <ul>
     * <li>most schema- or token-related methods in {@link Metadata} will return stale
     * or null/empty results (see the javadoc of each method for details);</li>
     * <li>{@link Metadata#newToken(String)} and
     * {@link Metadata#newTokenRange(Token, Token)} will throw an exception if metadata
     * was disabled before startup;</li>
     * <li>token-aware routing will not work properly: if metadata was never initialized,
     * {@link com.datastax.driver.core.policies.TokenAwarePolicy} will always delegate
     * to its child policy. Otherwise, it might not pick the best coordinator (i.e. chose
     * a host that is not a replica for the statement's routing key). In addition, statements
     * prepared while the metadata was disabled might also be sent to a non-optimal coordinator,
     * even if metadata was re-enabled later.</li>
     * </ul>
     *
     * @param enabled whether metadata is enabled.
     * @return this {@code QueryOptions} instance.
     */
    public QueryOptions setMetadataEnabled(boolean enabled) {
        boolean wasEnabled = this.metadataEnabled;
        this.metadataEnabled = enabled;
        if (!wasEnabled && enabled && manager != null) {
            manager.submitSchemaRefresh(null, null, null, null); // will also refresh token map
        }
        return this;
    }

    /**
     * Whether client-side token and schema metadata is enabled.
     *
     * @return the value.
     * @see #setMetadataEnabled(boolean)
     */
    public boolean isMetadataEnabled() {
        return metadataEnabled;
    }

    /**
     * Sets the default window size in milliseconds used to debounce node list refresh requests.
     * <p/>
     * When the control connection receives a new schema refresh request,
     * it puts it on hold and starts a timer, cancelling any previous running timer;
     * when a timer expires, then the pending requests are coalesced and executed
     * as a single request.
     *
     * @param refreshSchemaIntervalMillis The default window size in milliseconds used to debounce schema refresh requests.
     */
    public QueryOptions setRefreshSchemaIntervalMillis(int refreshSchemaIntervalMillis) {
        this.refreshSchemaIntervalMillis = refreshSchemaIntervalMillis;
        return this;
    }

    /**
     * The default window size in milliseconds used to debounce schema refresh requests.
     *
     * @return The default window size in milliseconds used to debounce schema refresh requests.
     */
    public int getRefreshSchemaIntervalMillis() {
        return refreshSchemaIntervalMillis;
    }

    /**
     * Sets the maximum number of schema refresh requests that the control connection can accumulate
     * before executing them.
     * <p/>
     * When the control connection receives a new schema refresh request,
     * it puts it on hold and starts a timer, cancelling any previous running timer;
     * if the control connection receives too many events, is parameter allows to trigger
     * execution of pending requests, event if the last timer is still running.
     *
     * @param maxPendingRefreshSchemaRequests The maximum number of schema refresh requests that the control connection can accumulate
     *                                        before executing them.
     */
    public QueryOptions setMaxPendingRefreshSchemaRequests(int maxPendingRefreshSchemaRequests) {
        this.maxPendingRefreshSchemaRequests = maxPendingRefreshSchemaRequests;
        return this;
    }

    /**
     * The maximum number of schema refresh requests that the control connection can accumulate
     * before executing them.
     *
     * @return The maximum number of schema refresh requests that the control connection can accumulate
     * before executing them.
     */
    public int getMaxPendingRefreshSchemaRequests() {
        return maxPendingRefreshSchemaRequests;
    }

    /**
     * Sets the default window size in milliseconds used to debounce node list refresh requests.
     * <p/>
     * When the control connection receives a new node list refresh request,
     * it puts it on hold and starts a timer, cancelling any previous running timer;
     * when a timer expires, then the pending requests are coalesced and executed
     * as a single request.
     *
     * @param refreshNodeListIntervalMillis The default window size in milliseconds used to debounce node list refresh requests.
     */
    public QueryOptions setRefreshNodeListIntervalMillis(int refreshNodeListIntervalMillis) {
        this.refreshNodeListIntervalMillis = refreshNodeListIntervalMillis;
        return this;
    }

    /**
     * The default window size in milliseconds used to debounce node list refresh requests.
     *
     * @return The default window size in milliseconds used to debounce node list refresh requests.
     */
    public int getRefreshNodeListIntervalMillis() {
        return refreshNodeListIntervalMillis;
    }

    /**
     * Sets the maximum number of node list refresh requests that the control connection can accumulate
     * before executing them.
     * <p/>
     * When the control connection receives a new node list refresh request,
     * it puts it on hold and starts a timer, cancelling any previous running timer;
     * if the control connection receives too many events, is parameter allows to trigger
     * execution of pending requests, event if the last timer is still running.
     *
     * @param maxPendingRefreshNodeListRequests The maximum number of node list refresh requests that the control connection can accumulate
     *                                          before executing them.
     */
    public QueryOptions setMaxPendingRefreshNodeListRequests(int maxPendingRefreshNodeListRequests) {
        this.maxPendingRefreshNodeListRequests = maxPendingRefreshNodeListRequests;
        return this;
    }

    /**
     * Sets the maximum number of node list refresh requests that the control connection can accumulate
     * before executing them.
     *
     * @return The maximum number of node list refresh requests that the control connection can accumulate
     * before executing them.
     */
    public int getMaxPendingRefreshNodeListRequests() {
        return maxPendingRefreshNodeListRequests;
    }

    /**
     * Sets the default window size in milliseconds used to debounce node refresh requests.
     * <p/>
     * When the control connection receives a new node refresh request,
     * it puts it on hold and starts a timer, cancelling any previous running timer;
     * when a timer expires, then the pending requests are coalesced and executed
     * as a single request.
     *
     * @param refreshNodeIntervalMillis The default window size in milliseconds used to debounce node refresh requests.
     */
    public QueryOptions setRefreshNodeIntervalMillis(int refreshNodeIntervalMillis) {
        this.refreshNodeIntervalMillis = refreshNodeIntervalMillis;
        return this;
    }

    /**
     * The default window size in milliseconds used to debounce node refresh requests.
     *
     * @return The default window size in milliseconds used to debounce node refresh requests.
     */
    public int getRefreshNodeIntervalMillis() {
        return refreshNodeIntervalMillis;
    }

    /**
     * Sets the maximum number of node refresh requests that the control connection can accumulate
     * before executing them.
     * <p/>
     * When the control connection receives a new node refresh request,
     * it puts it on hold and starts a timer, cancelling any previous running timer;
     * if the control connection receives too many events, is parameter allows to trigger
     * execution of pending requests, event if the last timer is still running.
     *
     * @param maxPendingRefreshNodeRequests The maximum number of node refresh requests that the control connection can accumulate
     *                                      before executing them.
     */
    public QueryOptions setMaxPendingRefreshNodeRequests(int maxPendingRefreshNodeRequests) {
        this.maxPendingRefreshNodeRequests = maxPendingRefreshNodeRequests;
        return this;
    }

    /**
     * The maximum number of node refresh requests that the control connection can accumulate
     * before executing them.
     *
     * @return The maximum number of node refresh requests that the control connection can accumulate
     * before executing them.
     */
    public int getMaxPendingRefreshNodeRequests() {
        return maxPendingRefreshNodeRequests;
    }

}
