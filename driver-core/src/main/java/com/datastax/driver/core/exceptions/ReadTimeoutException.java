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

import java.net.InetSocketAddress;

/**
 * A Cassandra timeout during a read query.
 */
public class ReadTimeoutException extends QueryConsistencyException {

    private static final long serialVersionUID = 0;

    private final boolean dataPresent;

    /**
     * This constructor should only be used internally by the driver
     * when decoding error responses.
     */
    public ReadTimeoutException(ConsistencyLevel consistency, int received, int required, boolean dataPresent) {
        this(null, consistency, received, required, dataPresent);
    }

    public ReadTimeoutException(InetSocketAddress address, ConsistencyLevel consistency, int received, int required, boolean dataPresent) {
        super(
                address,
                String.format("Cassandra timeout during read query at consistency %s (%s)", consistency, formatDetails(received, required, dataPresent)),
                consistency,
                received,
                required
        );
        this.dataPresent = dataPresent;
    }

    private ReadTimeoutException(InetSocketAddress address, String msg, Throwable cause, ConsistencyLevel consistency, int received, int required, boolean dataPresent) {
        super(address, msg, cause, consistency, received, required);
        this.dataPresent = dataPresent;
    }

    private static String formatDetails(int received, int required, boolean dataPresent) {
        if (received < required)
            return String.format("%d responses were required but only %d replica responded", required, received);
        else if (!dataPresent)
            return "the replica queried for data didn't respond";
        else
            return "timeout while waiting for repair of inconsistent replica";
    }

    /**
     * Whether the actual data was amongst the received replica responses.
     * <p/>
     * During reads, Cassandra doesn't request data from every replica to
     * minimize internal network traffic. Instead, some replicas are only asked
     * for a checksum of the data. A read timeout may occurred even if enough
     * replicas have responded to fulfill the consistency level if only checksum
     * responses have been received. This method allows to detect that case.
     *
     * @return whether the data was amongst the received replica responses.
     */
    public boolean wasDataRetrieved() {
        return dataPresent;
    }

    @Override
    public ReadTimeoutException copy() {
        return new ReadTimeoutException(
                getAddress(),
                getMessage(),
                this,
                getConsistencyLevel(),
                getReceivedAcknowledgements(),
                getRequiredAcknowledgements(),
                wasDataRetrieved()
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
    public ReadTimeoutException copy(InetSocketAddress address) {
        return new ReadTimeoutException(
                address,
                getMessage(),
                this,
                getConsistencyLevel(),
                getReceivedAcknowledgements(),
                getRequiredAcknowledgements(),
                wasDataRetrieved()
        );
    }

}
