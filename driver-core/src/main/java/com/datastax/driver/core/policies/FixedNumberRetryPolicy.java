package com.datastax.driver.core.policies;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;

/**
 * A fixed retry policy that will retry any query a given number of times regardless of any other factor such as number of replicas available.
 */
public class FixedNumberRetryPolicy implements RetryPolicy {
    private final int numberOfTimesToRetry;

    /**
     *
     * @param numberOfTimesToRetry the number of times each query is retried before rethrowing to the caller
     */
    public FixedNumberRetryPolicy(int numberOfTimesToRetry) {
        this.numberOfTimesToRetry = numberOfTimesToRetry;
    }
    /**
     * Defines whether to retry and at which consistency level on a read timeout.
     * <p>
     * This method triggers a configured maximum number of retries.
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
     * @return {@code RetryDecision.retry(cl)} if the configured number of retries have not happened,
     * {@code RetryDecision.rethrow()} otherwise.
     */
    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        return getRetryDecision(cl, nbRetry);
    }

    /**
     * Defines whether to retry and at which consistency level on a write timeout.
     * <p>
     * This method triggers a configured maximum number of retries.
     *
     * @param statement the original query that timed out.
     * @param cl the original consistency level of the write that timed out.
     * @param writeType the type of the write that timed out.
     * @param requiredAcks the number of acknowledgments that were required to
     * achieve the requested consistency level.
     * @param receivedAcks the number of acknowledgments that had been received
     * by the time the timeout exception was raised.
     * @param nbRetry the number of retry already performed for this operation.
     * @return {@code RetryDecision.retry(cl)} if the configured number of retries have not happened,
     * {@code RetryDecision.rethrow()} otherwise.
     */
    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        return getRetryDecision(cl, nbRetry);
    }

    /**
     * Defines whether to retry and at which consistency level on an
     * unavailable exception.
     * <p>
     * This method triggers a configured maximum number of retries.
     *
     * @param statement the original query for which the consistency level cannot
     * be achieved.
     * @param cl the original consistency level for the operation.
     * @param requiredReplica the number of replica that should have been
     * (known) alive for the operation to be attempted.
     * @param aliveReplica the number of replica that were know to be alive by
     * the coordinator of the operation.
     * @return {@code RetryDecision.retry(cl)} if the configured number of retries have not happened,
     * {@code RetryDecision.rethrow()} otherwise.
     */
    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        return getRetryDecision(cl, nbRetry);
    }

    private RetryDecision getRetryDecision(ConsistencyLevel cl, int nbRetry) {
        if (nbRetry >= numberOfTimesToRetry) {
            return RetryDecision.rethrow();
        }
        return RetryDecision.retry(cl);
    }
}
