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
     * Defines whether to retry and at which consistency level on a read timeout.
     * <p/>
     * This method triggers a maximum of one retry, and only if enough
     * replicas had responded to the read request but data was not retrieved
     * amongst those. Indeed, that case usually means that enough replica
     * are alive to satisfy the consistency but the coordinator picked a
     * dead one for data retrieval, not having detected that replica as dead
     * yet. The reasoning for retrying then is that by the time we get the
     * timeout the dead replica will likely have been detected as dead and
     * the retry has a high chance of success.
     *
     * @param statement         the original query that timed out.
     * @param cl                the original consistency level of the read that timed out.
     * @param requiredResponses the number of responses that were required to
     *                          achieve the requested consistency level.
     * @param receivedResponses the number of responses that had been received
     *                          by the time the timeout exception was raised.
     * @param dataRetrieved     whether actual data (by opposition to data checksum)
     *                          was present in the received responses.
     * @param nbRetry           the number of retries already performed for this operation.
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
     * Defines whether to retry and at which consistency level on a write timeout.
     * <p/>
     * This method triggers a maximum of one retry, and only in the case of
     * a {@code WriteType.BATCH_LOG} write. The reasoning for the retry in
     * that case is that write to the distributed batch log is tried by the
     * coordinator of the write against a small subset of all the nodes alive
     * in the local datacenter. Hence, a timeout usually means that none of
     * the nodes in that subset were alive but the coordinator hasn't
     * detected them as dead. By the time we get the timeout the dead
     * nodes will likely have been detected as dead and the retry has thus a
     * high chance of success.
     *
     * @param statement    the original query that timed out.
     * @param cl           the original consistency level of the write that timed out.
     * @param writeType    the type of the write that timed out.
     * @param requiredAcks the number of acknowledgments that were required to
     *                     achieve the requested consistency level.
     * @param receivedAcks the number of acknowledgments that had been received
     *                     by the time the timeout exception was raised.
     * @param nbRetry      the number of retry already performed for this operation.
     * @return {@code RetryDecision.retry(cl)} if no retry attempt has yet been tried and
     * {@code writeType == WriteType.BATCH_LOG}, {@code RetryDecision.rethrow()} otherwise.
     */
    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        if (nbRetry != 0)
            return RetryDecision.rethrow();

        // If the batch log write failed, retry the operation as this might just be we were unlucky at picking candidates
        return writeType == WriteType.BATCH_LOG ? RetryDecision.retry(cl) : RetryDecision.rethrow();
    }

    /**
     * Defines whether to retry and at which consistency level on an
     * unavailable exception.
     * <p/>
     * This method triggers a retry iff no retry has been executed before
     * (nbRetry == 0), with
     * {@link RetryPolicy.RetryDecision#tryNextHost(ConsistencyLevel) RetryDecision.tryNextHost(cl)},
     * otherwise it throws an exception. The retry will be processed on the next host
     * in the query plan according to the current Load Balancing Policy.
     * Where retrying on the same host in the event of an Unavailable exception
     * has almost no chance of success, if the first replica tried happens to
     * be "network" isolated from all the other nodes but can still answer to
     * the client, it makes sense to retry the query on another node.
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
        return (nbRetry == 0)
                ? RetryDecision.tryNextHost(cl)
                : RetryDecision.rethrow();
    }

    /**
     * {@inheritDoc}
     * <p/>
     * For historical reasons, this implementation triggers a retry on the next host in the query plan
     * with the same consistency level, regardless of the statement's idempotence.
     * Note that this breaks the general rule
     * stated in {@link RetryPolicy#onRequestError(Statement, ConsistencyLevel, DriverException, int)}:
     * "a retry should only be attempted if the request is known to be idempotent".
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
