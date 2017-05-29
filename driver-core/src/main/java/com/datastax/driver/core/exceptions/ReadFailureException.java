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
import java.util.Collections;
import java.util.Map;

/**
 * A non-timeout error during a read query.
 * <p/>
 * This happens when some of the replicas that were contacted by the coordinator replied with an error.
 */
@SuppressWarnings("serial")
public class ReadFailureException extends QueryConsistencyException {

    private final int failed;
    private final boolean dataPresent;
    private final Map<InetAddress, Integer> failuresMap;

    /**
     * This constructor should only be used internally by the driver
     * when decoding error responses.
     */
    public ReadFailureException(ConsistencyLevel consistency, int received, int required, int failed, Map<InetAddress, Integer> failuresMap, boolean dataPresent) {
        this(null, consistency, received, required, failed, failuresMap, dataPresent);
    }

    /**
     * @deprecated Legacy constructor for backward compatibility.
     */
    @Deprecated
    public ReadFailureException(ConsistencyLevel consistency, int received, int required, int failed, boolean dataPresent) {
        this(null, consistency, received, required, failed, Collections.<InetAddress, Integer>emptyMap(), dataPresent);
    }

    public ReadFailureException(InetSocketAddress address, ConsistencyLevel consistency, int received, int required, int failed, Map<InetAddress, Integer> failuresMap, boolean dataPresent) {
        super(address, String.format("Cassandra failure during read query at consistency %s "
                        + "(%d responses were required but only %d replica responded, %d failed)",
                consistency, required, received, failed),
                consistency,
                received,
                required);
        this.failed = failed;
        this.failuresMap = failuresMap;
        this.dataPresent = dataPresent;
    }

    /**
     * @deprecated Legacy constructor for backward compatibility.
     */
    @Deprecated
    public ReadFailureException(InetSocketAddress address, ConsistencyLevel consistency, int received, int required, int failed, boolean dataPresent) {
        this(address, consistency, received, required, failed, Collections.<InetAddress, Integer>emptyMap(), dataPresent);
    }

    private ReadFailureException(InetSocketAddress address, String msg, Throwable cause, ConsistencyLevel consistency, int received, int required, int failed, Map<InetAddress, Integer> failuresMap, boolean dataPresent) {
        super(address, msg, cause, consistency, received, required);
        this.failed = failed;
        this.failuresMap = failuresMap;
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
     * Returns the a failure reason code for each node that failed.
     * <p/>
     * At the time of writing, the existing reason codes are:
     * <ul>
     * <li>{@code 0x0000}: the error does not have a specific code assigned yet, or the cause is
     * unknown.</li>
     * <li>{@code 0x0001}: The read operation scanned too many tombstones (as defined by
     * {@code tombstone_failure_threshold} in {@code cassandra.yaml}, causing a
     * {@code TombstoneOverwhelmingException}.</li>
     * </ul>
     * (please refer to the Cassandra documentation for your version for the most up-to-date list
     * of errors)
     * <p/>
     * This feature is available for protocol v5 or above only. With lower protocol versions, the
     * map will always be empty.
     *
     * @return a map of IP addresses to failure codes.
     */
    public Map<InetAddress, Integer> getFailuresMap() {
        return failuresMap;
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
                getRequiredAcknowledgements(), getFailures(), getFailuresMap(), wasDataRetrieved());
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
                getFailuresMap(),
                dataPresent);
    }
}
