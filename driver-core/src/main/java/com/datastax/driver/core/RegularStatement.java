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
     *
     * @return the values to use for this statement or {@code null} if there is
     * no such values.
     *
     * @see SimpleStatement#SimpleStatement(String, Object...)
     */
    public abstract ByteBuffer[] getValues();

    @Override
    public String toString() {
        return getQueryString();
    }
}
