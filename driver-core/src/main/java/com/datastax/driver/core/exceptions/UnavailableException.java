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
package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * Exception thrown when the coordinator knows there is not enough replica
 * alive to perform a query with the requested consistency level.
 */
public class UnavailableException extends QueryExecutionException {

    private final ConsistencyLevel consistency;
    private final int required;
    private final int alive;

    public UnavailableException(ConsistencyLevel consistency, int required, int alive) {
        super(String.format("Not enough replica available for query at consistency %s (%d required but only %d alive)", consistency, required, alive));
        this.consistency = consistency;
        this.required = required;
        this.alive = alive;
    }

    private UnavailableException(String message, Throwable cause, ConsistencyLevel consistency, int required, int alive) {
        super(message, cause);
        this.consistency = consistency;
        this.required = required;
        this.alive = alive;
    }

    /**
     * The consistency level of the operation triggering this unavailable exception.
     *
     * @return the consistency level of the operation triggering this unavailable exception.
     */
    public ConsistencyLevel getConsistency() {
        return consistency;
    }

    /**
     * The number of replica acknowledgements/responses required to perform the
     * operation (with its required consistency level).
     *
     * @return the number of replica acknowledgements/responses required to perform the
     * operation.
     */
    public int getRequiredReplicas() {
        return required;
    }

    /**
     * The number of replica that were known to be alive by the Cassandra
     * coordinator node when it tried to execute the operation.
     *
     * @return The number of replica that were known to be alive by the Cassandra
     * coordinator node when it tried to execute the operation.
     */
    public int getAliveReplicas() {
        return alive;
    }

    public DriverException copy() {
        return new UnavailableException(getMessage(), this, consistency, required, alive);
    }
}
