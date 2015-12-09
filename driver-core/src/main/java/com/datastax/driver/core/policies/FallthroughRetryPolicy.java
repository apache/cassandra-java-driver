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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * A retry policy that never retry (nor ignore).
 * <p/>
 * All of the methods of this retry policy unconditionally return {@link RetryPolicy.RetryDecision#rethrow}.
 * If this policy is used, retry will have to be implemented in business code.
 */
public class FallthroughRetryPolicy implements RetryPolicy {

    public static final FallthroughRetryPolicy INSTANCE = new FallthroughRetryPolicy();

    private FallthroughRetryPolicy() {
    }

    /**
     * Defines whether to retry and at which consistency level on a read timeout.
     *
     * @param statement         the original query that timed out.
     * @param cl                the original consistency level of the read that timed out.
     * @param requiredResponses the number of responses that were required to
     *                          achieve the requested consistency level.
     * @param receivedResponses the number of responses that had been received
     *                          by the time the timeout exception was raised.
     * @param dataRetrieved     whether actual data (by opposition to data checksum)
     *                          was present in the received responses.
     * @param nbRetry           the number of retry already performed for this operation.
     * @return {@code RetryDecision.rethrow()}.
     */
    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        return RetryDecision.rethrow();
    }

    /**
     * Defines whether to retry and at which consistency level on a write timeout.
     *
     * @param statement    the original query that timed out.
     * @param cl           the original consistency level of the write that timed out.
     * @param writeType    the type of the write that timed out.
     * @param requiredAcks the number of acknowledgments that were required to
     *                     achieve the requested consistency level.
     * @param receivedAcks the number of acknowledgments that had been received
     *                     by the time the timeout exception was raised.
     * @param nbRetry      the number of retry already performed for this operation.
     * @return {@code RetryDecision.rethrow()}.
     */
    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        return RetryDecision.rethrow();
    }

    /**
     * Defines whether to retry and at which consistency level on an
     * unavailable exception.
     *
     * @param statement       the original query for which the consistency level cannot
     *                        be achieved.
     * @param cl              the original consistency level for the operation.
     * @param requiredReplica the number of replica that should have been
     *                        (known) alive for the operation to be attempted.
     * @param aliveReplica    the number of replica that were know to be alive by
     *                        the coordinator of the operation.
     * @param nbRetry         the number of retry already performed for this operation.
     * @return {@code RetryDecision.rethrow()}.
     */
    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        return RetryDecision.rethrow();
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation always returns {@code RetryDecision.rethrow()}.
     */
    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
        return RetryDecision.rethrow();
    }


    @Override
    public void init(Cluster cluster) {
        // nothing to do
    }

    @Override
    public void close() {
        // nothing to do
    }
}
