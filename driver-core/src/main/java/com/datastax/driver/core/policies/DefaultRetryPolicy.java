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
 * The default retry policy.
 * <p/>
 * This policy retries queries in only two cases:
 * <ul>
 * <li>On a read timeout, if enough replicas replied but data was not retrieved.</li>
 * <li>On a write timeout, if we timeout while writing the distributed log used by batch statements.</li>
 * </ul>
 * <p/>
 * This retry policy is conservative in that it will never retry with a
 * different consistency level than the one of the initial operation.
 * <p/>
 * In some cases, it may be convenient to use a more aggressive retry policy
 * like {@link DowngradingConsistencyRetryPolicy}.
 */
public class DefaultRetryPolicy implements RetryPolicy {

    public static final DefaultRetryPolicy INSTANCE = new DefaultRetryPolicy();

    private DefaultRetryPolicy() {
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation triggers a maximum of one retry, and only if enough
     * replicas had responded to the read request but data was not retrieved
     * amongst those. Indeed, that case usually means that enough replica
     * are alive to satisfy the consistency but the coordinator picked a
     * dead one for data retrieval, not having detected that replica as dead
     * yet. The reasoning for retrying then is that by the time we get the
     * timeout the dead replica will likely have been detected as dead and
     * the retry has a high chance of success.
     *
     * @return {@code RetryDecision.retry(cl)} if no retry attempt has yet been tried and
     * {@code receivedResponses >= requiredResponses && !dataRetrieved}, {@code RetryDecision.rethrow()} otherwise.
     */
    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        if (nbRetry != 0)
            return RetryDecision.rethrow();

        return receivedResponses >= requiredResponses && !dataRetrieved ? RetryDecision.retry(cl) : RetryDecision.rethrow();
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation triggers a maximum of one retry, and only in the case of
     * a {@code WriteType.BATCH_LOG} write. The reasoning for the retry in
     * that case is that write to the distributed batch log is tried by the
     * coordinator of the write against a small subset of all the nodes alive
     * in the local datacenter. Hence, a timeout usually means that none of
     * the nodes in that subset were alive but the coordinator hasn't
     * detected them as dead. By the time we get the timeout the dead
     * nodes will likely have been detected as dead and the retry has thus a
     * high chance of success.
     *
     * @return {@code RetryDecision.retry(cl)} if no retry attempt has yet been tried and
     * {@code writeType == WriteType.BATCH_LOG}, {@code RetryDecision.rethrow()} otherwise.
     */
    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        if (nbRetry != 0)
            return RetryDecision.rethrow();

        // If the batch log write failed, retry the operation as this might just be we were unlucky at picking candidates
        // JAVA-764: testing the write type automatically filters out serial consistency levels as these have always WriteType.CAS.
        return writeType == WriteType.BATCH_LOG ? RetryDecision.retry(cl) : RetryDecision.rethrow();
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation does the following:
     * <ul>
     * <li>if this is the first retry ({@code nbRetry == 0}), it triggers a retry on the next host in the query plan
     * with the same consistency level ({@link RetryPolicy.RetryDecision#tryNextHost(ConsistencyLevel) RetryDecision#tryNextHost(null)}.
     * The rationale is that the first coordinator might have been network-isolated from all other nodes (thinking
     * they're down), but still able to communicate with the client; in that case, retrying on the same host has almost
     * no chance of success, but moving to the next host might solve the issue.</li>
     * <li>otherwise, the exception is rethrow.</li>
     * </ul>
     */
    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        return (nbRetry == 0)
                ? RetryDecision.tryNextHost(null)
                : RetryDecision.rethrow();
    }

    /**
     * {@inheritDoc}
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
