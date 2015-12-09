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
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A retry policy that wraps another policy, logging the decision made by its sub-policy.
 * <p/>
 * Note that this policy only log the IGNORE and RETRY decisions (since
 * RETHROW decisions are just meant to propagate the cassandra exception). The
 * logging is done at the INFO level.
 */
public class LoggingRetryPolicy implements RetryPolicy {

    private static final Logger logger = LoggerFactory.getLogger(LoggingRetryPolicy.class);

    @VisibleForTesting
    static final String IGNORING_READ_TIMEOUT =
            "Ignoring read timeout (initial consistency: {}, required responses: {}, received responses: {}, data retrieved: {}, retries: {})";

    @VisibleForTesting
    static final String RETRYING_ON_READ_TIMEOUT =
            "Retrying on read timeout on {} at consistency {} (initial consistency: {}, required responses: {}, received responses: {}, data retrieved: {}, retries: {})";

    @VisibleForTesting
    static final String IGNORING_WRITE_TIMEOUT =
            "Ignoring write timeout (initial consistency: {}, write type: {}, required acknowledgments: {}, received acknowledgments: {}, retries: {})";

    @VisibleForTesting
    static final String RETRYING_ON_WRITE_TIMEOUT =
            "Retrying on write timeout on {} at consistency {} (initial consistency: {}, write type: {}, required acknowledgments: {}, received acknowledgments: {}, retries: {})";

    @VisibleForTesting
    static final String IGNORING_UNAVAILABLE =
            "Ignoring unavailable exception (initial consistency: {}, required replica: {}, alive replica: {}, retries: {})";

    @VisibleForTesting
    static final String RETRYING_ON_UNAVAILABLE =
            "Retrying on unavailable exception on {} at consistency {} (initial consistency: {}, required replica: {}, alive replica: {}, retries: {})";

    @VisibleForTesting
    static final String IGNORING_REQUEST_ERROR =
            "Ignoring request error (initial consistency: {}, retries: {}, exception: {})";

    @VisibleForTesting
    static final String RETRYING_ON_REQUEST_ERROR =
            "Retrying on request error on {} at consistency {} (initial consistency: {}, retries: {}, exception: {})";

    private final RetryPolicy policy;

    /**
     * Creates a new {@code RetryPolicy} that logs the decision of {@code policy}.
     *
     * @param policy the policy to wrap. The policy created by this constructor
     *               will return the same decision than {@code policy} but will log them.
     */
    public LoggingRetryPolicy(RetryPolicy policy) {
        this.policy = policy;
    }

    private static ConsistencyLevel cl(ConsistencyLevel cl, RetryDecision decision) {
        return decision.getRetryConsistencyLevel() == null ? cl : decision.getRetryConsistencyLevel();
    }

    private static String host(RetryDecision decision) {
        return decision.isRetryCurrent() ? "same host" : "next host";
    }

    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        RetryDecision decision = policy.onReadTimeout(statement, cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry);
        switch (decision.getType()) {
            case IGNORE:
                logDecision(IGNORING_READ_TIMEOUT, cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry);
                break;
            case RETRY:
                logDecision(RETRYING_ON_READ_TIMEOUT, host(decision), cl(cl, decision), cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry);
                break;
        }
        return decision;
    }

    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        RetryDecision decision = policy.onWriteTimeout(statement, cl, writeType, requiredAcks, receivedAcks, nbRetry);
        switch (decision.getType()) {
            case IGNORE:
                logDecision(IGNORING_WRITE_TIMEOUT, cl, writeType, requiredAcks, receivedAcks, nbRetry);
                break;
            case RETRY:
                logDecision(RETRYING_ON_WRITE_TIMEOUT, host(decision), cl(cl, decision), cl, writeType, requiredAcks, receivedAcks, nbRetry);
                break;
        }
        return decision;
    }

    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        RetryDecision decision = policy.onUnavailable(statement, cl, requiredReplica, aliveReplica, nbRetry);
        switch (decision.getType()) {
            case IGNORE:
                logDecision(IGNORING_UNAVAILABLE, cl, requiredReplica, aliveReplica, nbRetry);
                break;
            case RETRY:
                logDecision(RETRYING_ON_UNAVAILABLE, host(decision), cl(cl, decision), cl, requiredReplica, aliveReplica, nbRetry);
                break;
        }
        return decision;
    }

    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
        RetryDecision decision = policy.onRequestError(statement, cl, e, nbRetry);
        switch (decision.getType()) {
            case IGNORE:
                logDecision(IGNORING_REQUEST_ERROR, cl, nbRetry, e);
                break;
            case RETRY:
                logDecision(RETRYING_ON_REQUEST_ERROR, host(decision), cl(cl, decision), cl, nbRetry, e);
                break;
        }
        return decision;
    }

    @Override
    public void init(Cluster cluster) {
        // nothing to do
    }

    @Override
    public void close() {
        policy.close();
    }

    /**
     * Logs the decision according to the given template and parameters.
     * The log level is INFO, but subclasses may override.
     *
     * @param template   The template to use; arguments must be specified in SLF4J style, i.e. {@code "{}"}.
     * @param parameters The template parameters.
     */
    protected void logDecision(String template, Object... parameters) {
        logger.info(template, parameters);
    }


}
