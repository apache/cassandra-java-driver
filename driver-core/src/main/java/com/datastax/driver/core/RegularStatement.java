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

import java.nio.ByteBuffer;

/**
 * A regular (non-prepared and non batched) CQL statement.
 * <p>
 * This class represents a query string along with query options (and optionally
 * binary values, see {@code getValues}). It can be extended but {@link SimpleStatement}
 * is provided as a simple implementation to build a {@code RegularStatement} directly
 * from its query string.
 */
public abstract class RegularStatement extends Statement {

    /**
     * Creates a new RegularStatement.
     */
    protected RegularStatement() {}

    /**
     * Returns the query string for this statement.
     *
     * @return a valid CQL query string.
     */
    public abstract String getQueryString();

    /**
     * The values to use for this statement.
     * <p>
     * Note: Values for a RegularStatement (i.e. if this method does not return
     * {@code null}) are not supported with the native protocol version 1: you
     * will get an {@link UnsupportedProtocolVersionException} when submitting
     * one if version 1 of the protocol is in use (i.e. if you've force version
     * 1 through {@link Cluster.Builder#withProtocolVersion} or you use
     * Cassandra 1.2).
     *
     * @param protocolVersion the protocol version in which the returned values
     * must be serialized for.
     * @return the values to use for this statement or {@code null} if there is
     * no such values.
     *
     * @see SimpleStatement#SimpleStatement(String, Object...)
     */
    public abstract ByteBuffer[] getValues(ProtocolVersion protocolVersion);

    /**
     * Whether or not this statement has values, that is if {@code getValues}
     * will return {@code null} or not.
     *
     * @return {@code false} if {@link #getValues} returns {@code null}, {@code true}
     * otherwise.
     */
    public abstract boolean hasValues();

    @Override
    public String toString() {
        return getQueryString();
    }
}
