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
 * Options related to defaults for individual queries.
 */
public class QueryOptions {

    /**
     * The default consistency level for queries: {@code ConsistencyLevel.ONE}.
     */
    public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.ONE;

    /**
     * The default serial consistency level for conditional updates: {@code ConsistencyLevel.SERIAL}.
     */
    public static final ConsistencyLevel DEFAULT_SERIAL_CONSISTENCY_LEVEL = ConsistencyLevel.SERIAL;

    /**
     * The default fetch size for SELECT queries: 5000.
     */
    public static final int DEFAULT_FETCH_SIZE = 5000;

    private volatile ConsistencyLevel consistency = DEFAULT_CONSISTENCY_LEVEL;
    private volatile ConsistencyLevel serialConsistency = DEFAULT_SERIAL_CONSISTENCY_LEVEL;
    private volatile int fetchSize = DEFAULT_FETCH_SIZE;

    /**
     * Creates a new {@code QueryOptions} instance using the {@code DEFAULT_CONSISTENCY_LEVEL},
     * {@code DEFAULT_SERIAL_CONSISTENCY_LEVEL} and {@code DEFAULT_FETCH_SIZE}.
     */
    public QueryOptions() {}

    /**
     * Sets the default consistency level to use for queries.
     * <p>
     * The consistency level set through this method will be use for queries
     * that don't explicitely have a consistency level, i.e. when {@link Query#getConsistencyLevel}
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
     * that don't explicitely have a serial consistency level, i.e. when {@link Query#getSerialConsistencyLevel}
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
     * that don't explicitely have a fetch size, i.e. when {@link Query#getFetchSize}
     * is less or equal to 0.
     *
     * @param fetchSize the new fetch size to set as default.
     * @return this {@code QueryOptions} instance.
     */
    public QueryOptions setFetchSize(int fetchSize) {
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
}
