package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.EndPoint;

public class CASWriteUnknownException extends QueryConsistencyException {

    private static final long serialVersionUID = 0;

    /**
     * This constructor should only be used internally by the driver when decoding error responses.
     */
    public CASWriteUnknownException(
            ConsistencyLevel consistency, int received, int required) {
        this(null, consistency, received, required);
    }

    public CASWriteUnknownException(
            EndPoint endPoint,
            ConsistencyLevel consistency,
            int received,
            int required) {
        super(
                endPoint,
                String.format(
                        "Cassandra timeout during read query at consistency %s (%s)",
                        consistency, formatDetails(received, required)),
                consistency,
                received,
                required);
    }

    private CASWriteUnknownException(
            EndPoint endPoint,
            String msg,
            Throwable cause,
            ConsistencyLevel consistency,
            int received,
            int required) {
        super(endPoint, msg, cause, consistency, received, required);
    }

    /** TODO **/
    private static String formatDetails(int received, int required) {
        if (received < required)
            return String.format(
                    "%d responses were required but only %d replica responded", required, received);
        else return "timeout while waiting for repair of inconsistent replica";
    }

    @Override
    public CASWriteUnknownException copy() {
        return new CASWriteUnknownException(
                getEndPoint(),
                getMessage(),
                this,
                getConsistencyLevel(),
                getReceivedAcknowledgements(),
                getRequiredAcknowledgements());
    }

    /**
     * Create a copy of this exception with a nicer stack trace, and including the coordinator address
     * that caused this exception to be raised.
     *
     * <p>This method is mainly intended for internal use by the driver and exists mainly because:
     *
     * <ol>
     *   <li>the original exception was decoded from a response frame and at that time, the
     *       coordinator address was not available; and
     *   <li>the newly-created exception will refer to the current thread in its stack trace, which
     *       generally yields a more user-friendly stack trace that the original one.
     * </ol>
     *
     * @param endPoint The full address of the host that caused this exception to be thrown.
     * @return a copy/clone of this exception, but with the given host address instead of the original
     *     one.
     */
    public CASWriteUnknownException copy(EndPoint endPoint) {
        return new CASWriteUnknownException(
                endPoint,
                getMessage(),
                this,
                getConsistencyLevel(),
                getReceivedAcknowledgements(),
                getRequiredAcknowledgements());
    }
}
