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

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Exception thrown when the coordinator knows there is not enough replicas
 * alive to perform a query with the requested consistency level.
 */
public class UnavailableException extends QueryExecutionException implements CoordinatorException {

    private static final long serialVersionUID = 0;

    private final InetSocketAddress address;
    private final ConsistencyLevel consistency;
    private final int required;
    private final int alive;

    /**
     * This constructor should only be used internally by the driver
     * when decoding error responses.
     */
    public UnavailableException(ConsistencyLevel consistency, int required, int alive) {
        this(null, consistency, required, alive);
    }

    public UnavailableException(InetSocketAddress address, ConsistencyLevel consistency, int required, int alive) {
        super(String.format("Not enough replicas available for query at consistency %s (%d required but only %d alive)", consistency, required, alive));
        this.address = address;
        this.consistency = consistency;
        this.required = required;
        this.alive = alive;
    }

    private UnavailableException(InetSocketAddress address, String message, Throwable cause, ConsistencyLevel consistency, int required, int alive) {
        super(message, cause);
        this.address = address;
        this.consistency = consistency;
        this.required = required;
        this.alive = alive;
    }

    /**
     * The consistency level of the operation triggering this unavailable exception.
     *
     * @return the consistency level of the operation triggering this unavailable exception.
     */
    public ConsistencyLevel getConsistencyLevel() {
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
     * The number of replicas that were known to be alive by the Cassandra
     * coordinator node when it tried to execute the operation.
     *
     * @return The number of replicas that were known to be alive by the Cassandra
     * coordinator node when it tried to execute the operation.
     */
    public int getAliveReplicas() {
        return alive;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetAddress getHost() {
        return address.getAddress();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetSocketAddress getAddress() {
        return address;
    }

    @Override
    public UnavailableException copy() {
        return new UnavailableException(getAddress(), getMessage(), this, consistency, required, alive);
    }

    /**
     * Create a copy of this exception with a nicer stack trace, and including the coordinator
     * address that caused this exception to be raised.
     * <p/>
     * This method is mainly intended for internal use by the driver and exists mainly because:
     * <ol>
     * <li>the original exception was decoded from a response frame
     * and at that time, the coordinator address was not available; and</li>
     * <li>the newly-created exception will refer to the current thread in its stack trace,
     * which generally yields a more user-friendly stack trace that the original one.</li>
     * </ol>
     *
     * @param address The full address of the host that caused this exception to be thrown.
     * @return a copy/clone of this exception, but with the given host address instead of the original one.
     */
    public UnavailableException copy(InetSocketAddress address) {
        return new UnavailableException(address, getMessage(), this, consistency, required, alive);
    }
}
