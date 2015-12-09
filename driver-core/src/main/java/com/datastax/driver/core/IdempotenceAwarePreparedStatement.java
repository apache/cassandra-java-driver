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

/**
 * Interface that extends the {@link PreparedStatement} interface in order to prevent breaking
 * binary compatibility. This interface is removed in versions 3.x of the driver and the methods
 * are directly available from the {@link PreparedStatement} interface.
 */
public interface IdempotenceAwarePreparedStatement extends PreparedStatement {
    /**
     * Sets whether this statement is idempotent.
     * <p/>
     * See {@link com.datastax.driver.core.Statement#isIdempotent} for more explanations about this property.
     *
     * @param idempotent the new value.
     * @return this {@code IdempotenceAwarePreparedStatement} object.
     */
    public PreparedStatement setIdempotent(Boolean idempotent);

    /**
     * Whether this statement is idempotent, i.e. whether it can be applied multiple times
     * without changing the result beyond the initial application.
     * <p/>
     * See {@link com.datastax.driver.core.Statement#isIdempotent} for more explanations about this property.
     * <p/>
     * Please note that idempotence will be propagated to all {@link BoundStatement}s created from this prepared statement.
     *
     * @return whether this statement is idempotent, or {@code null} to use
     * {@link QueryOptions#getDefaultIdempotence()}.
     */
    public Boolean isIdempotent();
}
