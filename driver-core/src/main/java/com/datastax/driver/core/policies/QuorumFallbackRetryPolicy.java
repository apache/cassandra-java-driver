package com.datastax.driver.core.policies;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;

/**
 * A retry policy that sometimes retry with a lower consistency level than
 * the one initially requested. It follows the same logic as the
 * {@link DowngradingConsistencyRetryPolicy}, except that it will retry
 * a LOCAL_QUORUM request at QUORUM first, before finally retrying at the highest achievable level.
 */
public class QuorumFallbackRetryPolicy implements RetryPolicy {
    private DowngradingConsistencyRetryPolicy defaultPolicy = DowngradingConsistencyRetryPolicy.INSTANCE;
    public static final QuorumFallbackRetryPolicy INSTANCE = new QuorumFallbackRetryPolicy();

    private QuorumFallbackRetryPolicy() {}

    /**
     * Defines whether to retry and at which consistency level on an
     * unavailable exception.
     * <p>
     * This method triggers a maximum of two retries. If the consistency level is
     * initially set to LOCAL_QUORUM, it will first retry at QUORUM, then at the highest achievable level if
     * that fails. All other cases pass through to {@link DowngradingConsistencyRetryPolicy}.
     *
     * @param statement the original query for which the consistency level cannot
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
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        if (nbRetry == 0 && ConsistencyLevel.LOCAL_QUORUM == cl)
            return RetryDecision.retry(ConsistencyLevel.QUORUM);
        else if (nbRetry == 1 && ConsistencyLevel.QUORUM != cl)
            return defaultPolicy.maxLikelyToWorkCL(aliveReplica);
        else
            return defaultPolicy.onUnavailable(statement, cl, requiredReplica, aliveReplica, nbRetry);
    }

    /**
     * Defines whether to retry and at which consistency level on a read timeout.
     * This method passes through to {@link DowngradingConsistencyRetryPolicy}.
     *
     * @param statement the original query that timed out.
     * @param cl the original consistency level of the read that timed out.
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
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        return defaultPolicy.onReadTimeout(statement, cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry);
    }

    /**
     * Defines whether to retry and at which consistency level on a write timeout.
     * This method passes through to {@link DowngradingConsistencyRetryPolicy}.
     *
     * @param statement the original query that timed out.
     * @param cl the original consistency level of the write that timed out.
     * @param writeType the type of the write that timed out.
     * @param requiredAcks the number of acknowledgments that were required to
     * achieve the requested consistency level.
     * @param receivedAcks the number of acknowledgments that had been received
     * by the time the timeout exception was raised.
     * @param nbRetry the number of retry already performed for this operation.
     * @return a RetryDecision as defined above.
     */
    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        return defaultPolicy.onWriteTimeout(statement, cl, writeType, requiredAcks, receivedAcks, nbRetry);
    }
}