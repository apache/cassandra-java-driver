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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * A policy that defines a default behavior to adopt when a request fails.
 * <p/>
 * Such policy allows to centralize the handling of query retries, allowing to
 * minimize the need for exception catching/handling in business code.
 */
public interface RetryPolicy {

    /**
     * A retry decision to adopt on a Cassandra exception (read/write timeout
     * or unavailable exception).
     * <p/>
     * There are three possible decisions:
     * <ul>
     * <li>RETHROW: no retry should be attempted and an exception should be thrown.</li>
     * <li>RETRY: the operation will be retried. The consistency level of the
     * retry should be specified.</li>
     * <li>IGNORE: no retry should be attempted and the exception should be
     * ignored. In that case, the operation that triggered the Cassandra
     * exception will return an empty result set.</li>
     * </ul>
     */
    class RetryDecision {
        
        private static final RetryDecision RETHROW_DECISION = new RetryDecision(Type.RETHROW, null, true);
        private static final RetryDecision IGNORE_DECISION = new RetryDecision(Type.IGNORE, null, true);

        /**
         * The types of retry decisions.
         */
        public enum Type {
            RETRY, RETHROW, IGNORE
        }

        private final Type type;
        private final ConsistencyLevel retryCL;
        private final boolean retryCurrent;

        private RetryDecision(Type type, ConsistencyLevel retryCL, boolean retryCurrent) {
            this.type = type;
            this.retryCL = retryCL;
            this.retryCurrent = retryCurrent;
        }

        /**
         * The type of this retry decision.
         *
         * @return the type of this retry decision.
         */
        public Type getType() {
            return type;
        }

        /**
         * The consistency level for this retry decision.
         * This is only meaningful for {@code RETRY} decisions.
         * The consistency level is always {@code null} for an
         * {@code IGNORE} or a {@code RETHROW} decision;
         * for a {@code RETRY} decision, the consistency level can be {@code null},
         * in which case the retry is done at the same consistency level
         * as in the previous attempt.
         *
         * @return the consistency level for a retry decision.
         */
        public ConsistencyLevel getRetryConsistencyLevel() {
            return retryCL;
        }

        /**
         * Whether this decision is to retry the same host.
         * This is only meaningful for {@code RETRY} decisions.
         *
         * @return {@code true} if the decision is to retry the same host,
         * {@code false} otherwise. Default is {@code false}.
         */
        public boolean isRetryCurrent() {
            return retryCurrent;
        }

        /**
         * Creates a {@link RetryDecision.Type#RETHROW} retry decision.
         *
         * @return a {@link RetryDecision.Type#RETHROW} retry decision.
         */
        public static RetryDecision rethrow() {
            return RETHROW_DECISION;
        }

        /**
         * Creates a {@link RetryDecision.Type#RETRY} retry decision using
         * the same host and the provided consistency level.
         * <p/>
         * If the provided consistency level is {@code null}, the retry will be done at the same consistency level as
         * the previous attempt.
         * <p/>
         * Beware that {@link ConsistencyLevel#isSerial() serial} consistency levels
         * should never be passed to this method; attempting to do so would trigger an
         * {@link com.datastax.driver.core.exceptions.InvalidQueryException InvalidQueryException}.
         *
         * @param consistency the consistency level to use for the retry; if {@code null},
         *                    the same level as the previous attempt will be used.
         * @return a {@link RetryDecision.Type#RETRY} decision using
         * the same host and the provided consistency level
         */
        public static RetryDecision retry(ConsistencyLevel consistency) {
            return new RetryDecision(Type.RETRY, consistency, true);
        }

        /**
         * Creates an {@link RetryDecision.Type#IGNORE} retry decision.
         *
         * @return an {@link RetryDecision.Type#IGNORE} retry decision.
         */
        public static RetryDecision ignore() {
            return IGNORE_DECISION;
        }

        /**
         * Creates a {@link RetryDecision.Type#RETRY} retry decision using the next host
         * in the query plan, and using the provided consistency level.
         * <p/>
         * If the provided consistency level is {@code null}, the retry will be done at the same consistency level as
         * the previous attempt.
         * <p/>
         * Beware that {@link ConsistencyLevel#isSerial() serial} consistency levels
         * should never be passed to this method; attempting to do so would trigger an
         * {@link com.datastax.driver.core.exceptions.InvalidQueryException InvalidQueryException}.
         *
         * @param consistency the consistency level to use for the retry; if {@code null},
         *                    the same level as the previous attempt will be used.
         * @return a {@link RetryDecision.Type#RETRY} retry decision using the next host
         * in the query plan, and using the provided consistency level.
         */
        public static RetryDecision tryNextHost(ConsistencyLevel consistency) {
            return new RetryDecision(Type.RETRY, consistency, false);
        }

        @Override
        public String toString() {
            switch (type) {
                case RETRY:
                    String retryClDesc = (retryCL == null) ? "same CL" : retryCL.toString();
                    String hostDesc = retryCurrent ? "same" : "next";
                    return "Retry at " + retryClDesc + " on " + hostDesc + " host.";
                case RETHROW:
                    return "Rethrow";
                case IGNORE:
                    return "Ignore";
            }
            throw new AssertionError();
        }
    }

    /**
     * Defines whether to retry and at which consistency level on a read timeout.
     * <p/>
     * Note that this method may be called even if
     * {@code requiredResponses >= receivedResponses} if {@code dataPresent} is
     * {@code false} (see
     * {@link com.datastax.driver.core.exceptions.ReadTimeoutException#wasDataRetrieved}).
     *
     * @param statement         the original query that timed out.
     * @param cl                the requested consistency level of the read that timed out.
     *                          Note that this can never be a {@link ConsistencyLevel#isSerial() serial}
     *                          consistency level.
     * @param requiredResponses the number of responses that were required to
     *                          achieve the requested consistency level.
     * @param receivedResponses the number of responses that had been received
     *                          by the time the timeout exception was raised.
     * @param dataRetrieved     whether actual data (by opposition to data checksum)
     *                          was present in the received responses.
     * @param nbRetry           the number of retry already performed for this operation.
     * @return the retry decision. If {@code RetryDecision.RETHROW} is returned,
     * a {@link com.datastax.driver.core.exceptions.ReadTimeoutException} will
     * be thrown for the operation.
     */
    RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry);

    /**
     * Defines whether to retry and at which consistency level on a write timeout.
     * <p/>
     * Note that if a statement is {@link Statement#isIdempotent() not idempotent}, the driver will never retry it on a
     * write timeout (this method won't even be called).
     *
     * @param statement    the original query that timed out.
     * @param cl           the requested consistency level of the write that timed out.
     *                     If the timeout occurred at the "paxos" phase of a
     *                     <a href="https://docs.datastax.com/en/cassandra/2.1/cassandra/dml/dml_ltwt_transaction_c.html">Lightweight transaction</a>,
     *                     then {@code cl} will actually be the requested {@link ConsistencyLevel#isSerial() serial} consistency level.
     *                     <em>Beware that serial consistency levels should never be passed to a {@link RetryDecision RetryDecision} as this would
     *                     invariably trigger an {@link com.datastax.driver.core.exceptions.InvalidQueryException InvalidQueryException}</em>.
     *                     Also, when {@code cl} is {@link ConsistencyLevel#isSerial() serial}, then {@code writeType} is always {@link WriteType#CAS CAS}.
     * @param writeType    the type of the write that timed out.
     * @param requiredAcks the number of acknowledgments that were required to
     *                     achieve the requested consistency level.
     * @param receivedAcks the number of acknowledgments that had been received
     *                     by the time the timeout exception was raised.
     * @param nbRetry      the number of retry already performed for this operation.
     * @return the retry decision. If {@code RetryDecision.RETHROW} is returned,
     * a {@link com.datastax.driver.core.exceptions.WriteTimeoutException} will
     * be thrown for the operation.
     */
    RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry);

    /**
     * Defines whether to retry and at which consistency level on an
     * unavailable exception.
     *
     * @param statement       the original query for which the consistency level cannot
     *                        be achieved.
     * @param cl              the requested consistency level for the operation.
     *                        If the operation failed at the "paxos" phase of a
     *                        <a href="https://docs.datastax.com/en/cassandra/2.1/cassandra/dml/dml_ltwt_transaction_c.html">Lightweight transaction</a>,
     *                        then {@code cl} will actually be the requested {@link ConsistencyLevel#isSerial() serial} consistency level.
     *                        <em>Beware that serial consistency levels should never be passed to a {@link RetryDecision RetryDecision} as this would
     *                        invariably trigger an {@link com.datastax.driver.core.exceptions.InvalidQueryException InvalidQueryException}</em>.
     * @param requiredReplica the number of replica that should have been
     *                        (known) alive for the operation to be attempted.
     * @param aliveReplica    the number of replica that were know to be alive by
     *                        the coordinator of the operation.
     * @param nbRetry         the number of retry already performed for this operation.
     * @return the retry decision. If {@code RetryDecision.RETHROW} is returned,
     * an {@link com.datastax.driver.core.exceptions.UnavailableException} will
     * be thrown for the operation.
     */
    RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry);

    /**
     * Defines whether to retry and at which consistency level on an
     * unexpected error.
     * <p/>
     * This method might be invoked in the following situations:
     * <ol>
     * <li>On a client timeout, while waiting for the server response
     * (see {@link SocketOptions#getReadTimeoutMillis()});</li>
     * <li>On a connection error (socket closed, etc.);</li>
     * <li>When the contacted host replies with an {@code OVERLOADED} error or a {@code SERVER_ERROR}.</li>
     * </ol>
     * <p/>
     * Note that when such an error occurs, there is no guarantee that the mutation has been applied server-side or not.
     * Therefore, if a statement is {@link Statement#isIdempotent() not idempotent}, the driver will never retry it
     * (this method won't even be called).
     *
     * @param statement the original query that failed.
     * @param cl        the requested consistency level for the operation.
     *                  Note that this is not necessarily the achieved consistency level (if any),
     *                  and it is never a {@link ConsistencyLevel#isSerial() serial} one.
     * @param e         the exception that caused this request to fail.
     * @param nbRetry   the number of retries already performed for this operation.
     * @return the retry decision. If {@code RetryDecision.RETHROW} is returned,
     * the {@link DriverException} passed to this method will be thrown for the operation.
     */
    RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry);

    /**
     * Gets invoked at cluster startup.
     *
     * @param cluster the cluster that this policy is associated with.
     */
    void init(Cluster cluster);

    /**
     * Gets invoked at cluster shutdown.
     * <p/>
     * This gives the policy the opportunity to perform some cleanup, for instance stop threads that it might have started.
     */
    void close();
}
