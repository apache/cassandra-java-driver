/*
 *      Copyright (C) 2012 DataStax Inc.
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

import com.datastax.driver.core.*;

/**
 * A retry policy that sometimes retry with a lower consistency level than
 * the one initially requested.
 * <p>
 * <b>BEWARE</b>: This policy may retry queries using a lower consistency
 * level than the one initially requested. By doing so, it may break
 * consistency guarantees. In other words, if you use this retry policy,
 * there is cases (documented below) where a read at {@code QUORUM}
 * <b>may not</b> see a preceding write at {@code QUORUM}. Do not use this
 * policy unless you have understood the cases where this can happen and
 * are ok with that. It is also highly recommended to always wrap this
 * policy into {@link LoggingRetryPolicy} to log the occurrences of
 * such consistency break.
 * <p>
 * This policy implements the same retries than the {@link DefaultRetryPolicy}
 * policy. But on top of that, it also retries in the following cases:
 * <ul>
 *   <li>On a read timeout: if the number of replica that responded is
 *   greater than one but lower than is required by the requested
 *   consistency level, the operation is retried at a lower consistency
 *   level.</li>
 *   <li>On a write timeout: if the operation is an {@code
 *   WriteType.UNLOGGED_BATCH} and at least one replica acknowledged the
 *   write, the operation is retried at a lower consistency level.
 *   Furthermore, for other operation, if at least one replica acknowledged
 *   the write, the timeout is ignored.</li>
 *   <li>On an unavailable exception: if at least one replica is alive, the
 *   operation is retried at a lower consistency level.</li>
 * </ul>
 * <p>
 * The reasoning being this retry policy is the following one. If, based
 * on the information the Cassandra coordinator node returns, retrying the
 * operation with the initially requested consistency has a change to
 * succeed, do it. Otherwise, if based on these information we know <b>the
 * initially requested consistency level cannot be achieve currently</b>, then:
 * <ul>
 *   <li>For writes, ignore the exception (thus silently failing the
 *   consistency requirement) if we know the write has been persisted on at
 *   least one replica.</li>
 *   <li>For reads, try reading at a lower consistency level (thus silently
 *   failing the consistency requirement).</li>
 * </ul>
 * In other words, this policy implements the idea that if the requested
 * consistency level cannot be achieved, the next best thing for writes is
 * to make sure the data is persisted, and that reading something is better
 * than reading nothing, even if there is a risk of reading stale data.
 */
public class DowngradingConsistencyRetryPolicy implements RetryPolicy {

    public static final DowngradingConsistencyRetryPolicy INSTANCE = new DowngradingConsistencyRetryPolicy();

    private DowngradingConsistencyRetryPolicy() {}

    private RetryDecision maxLikelyToWorkCL(int knownOk) {
        if (knownOk >= 3)
            return RetryDecision.retry(ConsistencyLevel.THREE);
        else if (knownOk >= 2)
            return RetryDecision.retry(ConsistencyLevel.TWO);
        else if (knownOk >= 1)
            return RetryDecision.retry(ConsistencyLevel.ONE);
        else
            return RetryDecision.rethrow();
    }

    /**
     * Defines whether to retry and at which consistency level on a read timeout.
     * <p>
     * This method triggers a maximum of one retry. If less replica
     * responded than required by the consistency level (but at least one
     * replica did respond), the operation is retried at a lower
     * consistency level. If enough replica responded but data was not
     * retrieve, the operation is retried with the initial consistency
     * level. Otherwise, an exception is thrown.
     *
     * @param query the original query that timeouted.
     * @param cl the original consistency level of the read that timeouted.
     * @param requiredResponses the number of responses that were required to
     * achieve the requested consistency level.
     * @param receivedResponses the number of responses that had been received
     * by the time the timeout exception was raised.
     * @param dataRetrieved whether actual data (by opposition to data checksum)
     * was present in the received responses.
     * @param nbRetry the number of retry already performed for this operation.
     * @return a RetryDecision as defined above.
     */
    @Override
    public RetryDecision onReadTimeout(Query query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        if (nbRetry != 0)
            return RetryDecision.rethrow();

        if (receivedResponses < requiredResponses) {
            // Tries the biggest CL that is expected to work
            return maxLikelyToWorkCL(receivedResponses);
        }

        return !dataRetrieved ? RetryDecision.retry(cl) : RetryDecision.rethrow();
    }

    /**
     * Defines whether to retry and at which consistency level on a write timeout.
     * <p>
     * This method triggers a maximum of one retry. If {@code writeType ==
     * WriteType.BATCH_LOG}, the write is retried with the initial
     * consistency level. If {@code writeType == WriteType.UNLOGGED_BATCH}
     * and at least one replica acknowledged, the write is retried with a
     * lower consistency level (with unlogged batch, a write timeout can
     * <b>always</b> mean that part of the batch haven't been persisted at
     * all, even if {@code receivedAcks > 0}). For other {@code writeType},
     * if we know the write has been persisted on at least one replica, we
     * ignore the exception. Otherwise, an exception is thrown.
     *
     * @param query the original query that timeouted.
     * @param cl the original consistency level of the write that timeouted.
     * @param writeType the type of the write that timeouted.
     * @param requiredAcks the number of acknowledgments that were required to
     * achieve the requested consistency level.
     * @param receivedAcks the number of acknowledgments that had been received
     * by the time the timeout exception was raised.
     * @param nbRetry the number of retry already performed for this operation.
     * @return a RetryDecision as defined above.
     */
    @Override
    public RetryDecision onWriteTimeout(Query query, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        if (nbRetry != 0)
            return RetryDecision.rethrow();

        switch (writeType) {
            case SIMPLE:
            case BATCH:
                // Since we provide atomicity there is no point in retrying
                return RetryDecision.ignore();
            case COUNTER:
                // We should not retry counters, period!
                return RetryDecision.ignore();
            case UNLOGGED_BATCH:
                // Since only part of the batch could have been persisted,
                // retry with whatever consistency should allow to persist all
                return maxLikelyToWorkCL(receivedAcks);
            case BATCH_LOG:
                return RetryDecision.retry(cl);
        }
        return RetryDecision.rethrow();
    }

    /**
     * Defines whether to retry and at which consistency level on an
     * unavailable exception.
     * <p>
     * This method triggers a maximum of one retry. If at least one replica
     * is know to be alive, the operation is retried at a lower consistency
     * level.
     *
     * @param query the original query for which the consistency level cannot
     * be achieved.
     * @param cl the original consistency level for the operation.
     * @param requiredReplica the number of replica that should have been
     * (known) alive for the operation to be attempted.
     * @param aliveReplica the number of replica that were know to be alive by
     * the coordinator of the operation.
     * @param nbRetry the number of retry already performed for this operation.
     * @return a RetryDecision as defined above.
     */
    @Override
    public RetryDecision onUnavailable(Query query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        if (nbRetry != 0)
            return RetryDecision.rethrow();

        // Tries the biggest CL that is expected to work
        return maxLikelyToWorkCL(aliveReplica);
    }
}
