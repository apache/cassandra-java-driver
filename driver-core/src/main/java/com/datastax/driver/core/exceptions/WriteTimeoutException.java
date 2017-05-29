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
import com.datastax.driver.core.WriteType;

import java.net.InetSocketAddress;

/**
 * A Cassandra timeout during a write query.
 */
public class WriteTimeoutException extends QueryConsistencyException {

    private static final long serialVersionUID = 0;

    private final WriteType writeType;

    /**
     * This constructor should only be used internally by the driver
     * when decoding error responses.
     */
    public WriteTimeoutException(ConsistencyLevel consistency, WriteType writeType, int received, int required) {
        this(null, consistency, writeType, received, required);
    }

    public WriteTimeoutException(InetSocketAddress address, ConsistencyLevel consistency, WriteType writeType, int received, int required) {
        super(
                address,
                String.format("Cassandra timeout during write query at consistency %s (%d replica were required but only %d acknowledged the write)", consistency, required, received),
                consistency,
                received,
                required);
        this.writeType = writeType;
    }

    private WriteTimeoutException(InetSocketAddress address, String msg, Throwable cause, ConsistencyLevel consistency, WriteType writeType, int received, int required) {
        super(address, msg, cause, consistency, received, required);
        this.writeType = writeType;
    }

    /**
     * The type of the write for which a timeout was raised.
     *
     * @return the type of the write for which a timeout was raised.
     */
    public WriteType getWriteType() {
        return writeType;
    }

    @Override
    public WriteTimeoutException copy() {
        return new WriteTimeoutException(
                getAddress(),
                getMessage(),
                this,
                getConsistencyLevel(),
                getWriteType(),
                getReceivedAcknowledgements(),
                getRequiredAcknowledgements()
        );
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
    public WriteTimeoutException copy(InetSocketAddress address) {
        return new WriteTimeoutException(
                address,
                getMessage(),
                this,
                getConsistencyLevel(),
                getWriteType(),
                getReceivedAcknowledgements(),
                getRequiredAcknowledgements()
        );
    }

}
