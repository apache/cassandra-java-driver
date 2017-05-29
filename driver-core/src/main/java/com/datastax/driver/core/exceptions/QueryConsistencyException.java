/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.ConsistencyLevel;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * A failure to reach the required consistency level during the execution of a query.
 * <p/>
 * Such an exception is returned when the query has been tried by Cassandra but
 * cannot be achieved with the requested consistency level because either:
 * <ul>
 * <li>the coordinator did not receive enough replica responses within the rpc timeout
 * set for Cassandra;</li>
 * <li>some replicas replied with an error.</li>
 * </ul>.
 */
@SuppressWarnings("serial")
public abstract class QueryConsistencyException extends QueryExecutionException implements CoordinatorException {

    private final InetSocketAddress address;
    private final ConsistencyLevel consistency;
    private final int received;
    private final int required;

    protected QueryConsistencyException(InetSocketAddress address, String msg, ConsistencyLevel consistency, int received, int required) {
        super(msg);
        this.address = address;
        this.consistency = consistency;
        this.received = received;
        this.required = required;
    }

    protected QueryConsistencyException(InetSocketAddress address, String msg, Throwable cause, ConsistencyLevel consistency, int received, int required) {
        super(msg, cause);
        this.address = address;
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

    /**
     * The coordinator host that caused this exception to be thrown.
     * Note that this is the query coordinator host, <em>not</em> the host which timed out.
     *
     * @return The coordinator host that caused this exception to be thrown, or {@code null} if this exception has been generated driver-side.
     */
    @Override
    public InetAddress getHost() {
        return address.getAddress();
    }

    /**
     * The full address of the coordinator host that caused this exception to be thrown.
     * Note that this is the query coordinator host, <em>not</em> the host which timed out.
     *
     * @return the full address of the coordinator host that caused this exception to be thrown,
     * or {@code null} if this exception has been generated driver-side.
     */
    @Override
    public InetSocketAddress getAddress() {
        return address;
    }
}
