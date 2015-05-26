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
package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * A failure to reach the required consistency level during the execution of a query.
 *
 * Such an exception is returned when the query has been tried by Cassandra but
 * cannot be achieved with the requested consistency level because either:
 * <ul>
 *     <li>the coordinator did not receive enough replica responses within the rpc timeout
 *     set for Cassandra;</li>
 *     <li>some replicas replied with an error.</li>
 * </ul>.
 */
@SuppressWarnings("serial")
public abstract class QueryConsistencyException extends QueryExecutionException {

    private final ConsistencyLevel consistency;
    private final int received;
    private final int required;

    protected QueryConsistencyException(String msg, ConsistencyLevel consistency, int received, int required) {
        super(msg);
        this.consistency = consistency;
        this.received = received;
        this.required = required;
    }

    protected QueryConsistencyException(String msg, Throwable cause, ConsistencyLevel consistency, int received, int required) {
        super(msg, cause);
        this.consistency = consistency;
        this.received = received;
        this.required = required;
    }

    /**
     * The consistency level of the operation that failed.
     *
     * @return the consistency level of the operation that failed.
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistency;
    }

    /**
     * The number of replica that had acknowledged/responded to the operation
     * before it failed.
     *
     * @return the number of replica that had acknowledged/responded the
     * operation before it failed.
     */
    public int getReceivedAcknowledgements() {
        return received;
    }

    /**
     * The minimum number of replica acknowledgements/responses that were
     * required to fulfill the operation.
     *
     * @return The minimum number of replica acknowledgements/response that
     * were required to fulfill the operation.
     */
    public int getRequiredAcknowledgements() {
        return required;
    }
}
