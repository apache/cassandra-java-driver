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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * A retry policy that sometimes retries with a lower consistency level than
 * the one initially requested.
 * <p/>
 * <b>BEWARE</b>: this policy may retry queries using a lower consistency
 * level than the one initially requested. By doing so, it may break
 * consistency guarantees. In other words, if you use this retry policy,
 * there are cases (documented below) where a read at {@code QUORUM}
 * <b>may not</b> see a preceding write at {@code QUORUM}. Do not use this
 * policy unless you have understood the cases where this can happen and
 * are ok with that. It is also highly recommended to always wrap this
 * policy into {@link LoggingRetryPolicy} to log the occurrences of
 * such consistency breaks.
 * <p/>
 * This policy implements the same retries than the {@link DefaultRetryPolicy}
 * policy. But on top of that, it also retries in the following cases:
 * <ul>
 * <li>On a read timeout: if the number of replicas that responded is
 * greater than one, but lower than is required by the requested
 * consistency level, the operation is retried at a lower consistency
 * level.</li>
 * <li>On a write timeout: if the operation is a {@code
 * WriteType.UNLOGGED_BATCH} and at least one replica acknowledged the
 * write, the operation is retried at a lower consistency level.
 * Furthermore, for other operations, if at least one replica acknowledged
 * the write, the timeout is ignored.</li>
 * <li>On an unavailable exception: if at least one replica is alive, the
 * operation is retried at a lower consistency level.</li>
 * </ul>
 * The lower consistency level to use for retries is determined by the following rules:
 * <ul>
 * <li>if more than 3 replicas responded, use {@code THREE}.</li>
 * <li>if 1, 2 or 3 replicas responded, use the corresponding level {@code ONE}, {@code TWO} or {@code THREE}.</li>
 * </ul>
 * Note that if the initial consistency level was {@code EACH_QUORUM}, Cassandra returns the number of live replicas
 * <em>in the datacenter that failed to reach consistency</em>, not the overall number in the cluster. Therefore if this
 * number is 0, we still retry at {@code ONE}, on the assumption that a host may still be up in another datacenter.
 * <p/>
 * The reasoning being this retry policy is the following one. If, based
 * on the information the Cassandra coordinator node returns, retrying the
 * operation with the initially requested consistency has a chance to
 * succeed, do it. Otherwise, if based on this information we know <b>the
 * initially requested consistency level cannot be achieve currently</b>, then:
 * <ul>
 * <li>For writes, ignore the exception (thus silently failing the
 * consistency requirement) if we know the write has been persisted on at
 * least one replica.</li>
 * <li>For reads, try reading at a lower consistency level (thus silently
 * failing the consistency requirement).</li>
 * </ul>
 * In other words, this policy implements the idea that if the requested
 * consistency level cannot be achieved, the next best thing for writes is
 * to make sure the data is persisted, and that reading something is better
 * than reading nothing, even if there is a risk of reading stale data.
 */
public class DowngradingConsistencyRetryPolicy implements RetryPolicy {

    public static final DowngradingConsistencyRetryPolicy INSTANCE = new DowngradingConsistencyRetryPolicy();

    private DowngradingConsistencyRetryPolicy() {
    }

    private RetryDecision maxLikelyToWorkCL(int knownOk, ConsistencyLevel currentCL) {
        if (knownOk >= 3)
            return RetryDecision.retry(ConsistencyLevel.THREE);

        if (knownOk == 2)
            return RetryDecision.retry(ConsistencyLevel.TWO);

        // JAVA-1005: EACH_QUORUM does not report a global number of alive replicas
        // so even if we get 0 alive replicas, there might be
        // a node up in some other datacenter
        if (knownOk == 1 || currentCL == ConsistencyLevel.EACH_QUORUM)
            return RetryDecision.retry(ConsistencyLevel.ONE);
        
        return RetryDecision.rethrow();
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation triggers a maximum of one retry. If less replica
     * responded than required by the consistency level (but at least one
     * replica did respond), the operation is retried at a lower
     * consistency level. If enough replica responded but data was not
     * retrieve, the operation is retried with the initial consistency
     * level. Otherwise, an exception is thrown.
     */
    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        if (nbRetry != 0)
            return RetryDecision.rethrow();

        // CAS reads are not all that useful in terms of visibility of the writes since CAS write supports the
        // normal consistency levels on the committing phase. So the main use case for CAS reads is probably for
        // when you've timed out on a CAS write and want to make sure what happened. Downgrading in that case
        // would be always wrong so we just special case to rethrow.
        if (cl.isSerial())
            return RetryDecision.rethrow();

        if (receivedResponses < requiredResponses) {
            // Tries the biggest CL that is expected to work
            return maxLikelyToWorkCL(receivedResponses, cl);
        }

        return !dataRetrieved ? RetryDecision.retry(cl) : RetryDecision.rethrow();
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation triggers a maximum of one retry. If {@code writeType ==
     * WriteType.BATCH_LOG}, the write is retried with the initial
     * consistency level. If {@code writeType == WriteType.UNLOGGED_BATCH}
     * and at least one replica acknowledged, the write is retried with a
     * lower consistency level (with unlogged batch, a write timeout can
     * <b>always</b> mean that part of the batch haven't been persisted at
     * all, even if {@code receivedAcks > 0}). For other write types ({@code WriteType.SIMPLE}
     * and {@code WriteType.BATCH}), if we know the write has been persisted on at
     * least one replica, we ignore the exception. Otherwise, an exception is thrown.
     */
    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        if (nbRetry != 0)
            return RetryDecision.rethrow();

        switch (writeType) {
            case SIMPLE:
            case BATCH:
                // Since we provide atomicity there is no point in retrying
                return receivedAcks > 0 ? RetryDecision.ignore() : RetryDecision.rethrow();
            case UNLOGGED_BATCH:
                // Since only part of the batch could have been persisted,
                // retry with whatever consistency should allow to persist all
                return maxLikelyToWorkCL(receivedAcks, cl);
            case BATCH_LOG:
                return RetryDecision.retry(cl);
        }
        // We want to rethrow on COUNTER and CAS, because in those case "we don't know" and don't want to guess
        return RetryDecision.rethrow();
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation triggers a maximum of one retry. If at least one replica
     * is know to be alive, the operation is retried at a lower consistency
     * level.
     */
    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        if (nbRetry != 0)
            return RetryDecision.rethrow();

        // JAVA-764: if the requested consistency level is serial, it means that the operation failed at the paxos phase of a LWT.
        // Retry on the next host, on the assumption that the initial coordinator could be network-isolated.
        if (cl.isSerial())
            return RetryDecision.tryNextHost(null);

        // Tries the biggest CL that is expected to work
        return maxLikelyToWorkCL(aliveReplica, cl);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * For historical reasons, this implementation triggers a retry on the next host in the query plan
     * with the same consistency level, regardless of the statement's idempotence.
     * Note that this breaks the general rule
     * stated in {@link RetryPolicy#onRequestError(Statement, ConsistencyLevel, DriverException, int)}:
     * "a retry should only be attempted if the request is known to be idempotent".`
     */
    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
        return RetryDecision.tryNextHost(cl);
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
