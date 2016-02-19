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

import java.net.InetSocketAddress;

/**
 * A non-timeout error during a read query.
 * <p/>
 * This happens when some of the replicas that were contacted by the coordinator replied with an error.
 */
@SuppressWarnings("serial")
public class ReadFailureException extends QueryConsistencyException {

    private final int failed;
    private final boolean dataPresent;

    /**
     * This constructor should only be used internally by the driver
     * when decoding error responses.
     */
    public ReadFailureException(ConsistencyLevel consistency, int received, int required, int failed, boolean dataPresent) {
        this(null, consistency, received, required, failed, dataPresent);
    }

    public ReadFailureException(InetSocketAddress address, ConsistencyLevel consistency, int received, int required, int failed, boolean dataPresent) {
        super(address, String.format("Cassandra failure during read query at consistency %s "
                                + "(%d responses were required but only %d replica responded, %d failed)",
                        consistency, required, received, failed),
                consistency,
                received,
                required);
        this.failed = failed;
        this.dataPresent = dataPresent;
    }

    private ReadFailureException(InetSocketAddress address, String msg, Throwable cause, ConsistencyLevel consistency, int received, int required, int failed, boolean dataPresent) {
        super(address, msg, cause, consistency, received, required);
        this.failed = failed;
        this.dataPresent = dataPresent;
    }

    /**
     * Returns the number of replicas that experienced a failure while executing the request.
     *
     * @return the number of failures.
     */
    public int getFailures() {
        return failed;
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
    public ReadFailureException copy() {
        return new ReadFailureException(getAddress(), getMessage(), this, getConsistencyLevel(), getReceivedAcknowledgements(),
                getRequiredAcknowledgements(), getFailures(), wasDataRetrieved());
    }

    public ReadFailureException copy(InetSocketAddress address) {
        return new ReadFailureException(
                address,
                getMessage(),
                this,
                getConsistencyLevel(),
                getReceivedAcknowledgements(),
                getRequiredAcknowledgements(),
                failed,
                dataPresent);
    }
}
