package com.datastax.driver.core.policies;

import com.datastax.driver.core.*;

/**
 * A retry policy that never retry (nor ignore).
 * <p>
 * All of the methods of this retry policy unconditionally return {@link RetryPolicy.RetryDecision#rethrow}.
 * If this policy is used, retry will have to be implemented in business code.
 */
public class FallthroughRetryPolicy {

    public static final FallthroughRetryPolicy INSTANCE = new FallthroughRetryPolicy();

    private FallthroughRetryPolicy() {}

    public RetryPolicy.RetryDecision onReadTimeout(ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        return RetryPolicy.RetryDecision.rethrow();
    }

    public RetryPolicy.RetryDecision onWriteTimeout(ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        return RetryPolicy.RetryDecision.rethrow();
    }

    public RetryPolicy.RetryDecision onUnavailable(ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        return RetryPolicy.RetryDecision.rethrow();
    }
}
