package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * A Cassandra timeout during a read query.
 */
public class ReadTimeoutException extends QueryTimeoutException {

    private final boolean dataPresent;

    public ReadTimeoutException(ConsistencyLevel consistency, int received, int required, boolean dataPresent) {
        super(String.format("Cassandra timeout during read query at consistency %s (%s)", consistency, formatDetails(received, required, dataPresent)),
              consistency,
              received,
              required);
        this.dataPresent = dataPresent;
    }

    private static String formatDetails(int received, int required, boolean dataPresent) {
        if (received < required)
            return String.format("%d replica responded over %d required", received, required);
        else if (!dataPresent)
            return String.format("the replica queried for data didn't responded");
        else
            return String.format("timeout while waiting for repair of inconsistent replica");
    }

    /**
     * Whether the actual data was amongst the received replica responses.
     *
     * During reads, Cassandra doesn't request data from every replica to
     * minimize internal network traffic. Instead, some replica are only asked
     * for a checksum of the data. A read timeout may occured even if enough
     * replica have responded to fulfill the consistency level if only checksum
     * responses have been received. This method allow to detect that case.
     *
     * @return {@code true} if the data was amongst the received replica
     * responses, {@code false} otherwise.
     */
    public boolean wasDataRetrieved() {
        return dataPresent;
    }
}
