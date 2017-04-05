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
 * A retry policy that avoids retrying non-idempotent statements.
 * <p/>
 * In case of write timeouts or unexpected errors, this policy will always return {@link com.datastax.driver.core.policies.RetryPolicy.RetryDecision#rethrow()}
 * if the statement is deemed non-idempotent (see {@link #isIdempotent(Statement)}).
 * <p/>
 * For all other cases, this policy delegates the decision to the child policy.
 *
 * @deprecated As of version 3.1.0, the driver doesn't retry non-idempotent statements for write timeouts or unexpected
 * errors anymore. It is no longer necessary to wrap your retry policies in this policy.
 */
@Deprecated
public class IdempotenceAwareRetryPolicy implements RetryPolicy {

    private final RetryPolicy childPolicy;

    private QueryOptions queryOptions;

    /**
     * Creates a new instance.
     *
     * @param childPolicy the policy to wrap.
     */
    public IdempotenceAwareRetryPolicy(RetryPolicy childPolicy) {
        this.childPolicy = childPolicy;
    }

    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        return childPolicy.onReadTimeout(statement, cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry);
    }

    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        if (isIdempotent(statement))
            return childPolicy.onWriteTimeout(statement, cl, writeType, requiredAcks, receivedAcks, nbRetry);
        else
            return RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        return childPolicy.onUnavailable(statement, cl, requiredReplica, aliveReplica, nbRetry);
    }

    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
        if (isIdempotent(statement))
            return childPolicy.onRequestError(statement, cl, e, nbRetry);
        else
            return RetryDecision.rethrow();
    }

    @Override
    public void init(Cluster cluster) {
        childPolicy.init(cluster);
        queryOptions = cluster.getConfiguration().getQueryOptions();
    }

    @Override
    public void close() {
        childPolicy.close();
    }

    /**
     * Determines whether the given statement is idempotent or not.
     * <p/>
     * The current implementation inspects the statement's
     * {@link Statement#isIdempotent() idempotent flag};
     * if this flag is not set, then it inspects
     * {@link QueryOptions#getDefaultIdempotence()}.
     * <p/>
     * Subclasses may override if they have better knowledge of
     * the statement being executed.
     *
     * @param statement The statement to execute.
     * @return {@code true} if the given statement is idempotent,
     * {@code false} otherwise
     */
    protected boolean isIdempotent(Statement statement) {
        Boolean myValue = statement.isIdempotent();
        if (myValue != null)
            return myValue;
        else
            return queryOptions.getDefaultIdempotence();
    }

}
