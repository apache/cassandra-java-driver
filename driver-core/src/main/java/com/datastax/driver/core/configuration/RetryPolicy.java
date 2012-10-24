package com.datastax.driver.core.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.log4j.Level;

import com.datastax.driver.core.*;

/**
 * A policy that defines a default behavior to adopt when a request returns
 * a TimeoutException or an UnavailableException.
 *
 * Such policy allows to centralize the handling of query retries, allowing to
 * minimize the need for exception catching/handling in business code.
 */
public interface RetryPolicy {

    /**
     * A retry decision to adopt on a Cassandra exception (read/write timeout
     * or unavailable exception).
     * <p>
     * There is three possible decision:
     * <ul>
     *   <li>RETHROW: no retry should be attempted and an exception should be thrown</li>
     *   <li>RETRY: the operation will be retried. The consistency level of the
     *   retry should be specified.</li>
     *   <li>IGNORE: no retry should be attempted and the exception should be
     *   ignored. In that case, the operation that triggered the Cassandra
     *   exception will return an empty result set.</li>
     * </ul>
     */
    public static class RetryDecision {
        /**
         * The type of retry decisions.
         */
        public static enum Type { RETRY, RETHROW, IGNORE };

        private final Type type;
        private final ConsistencyLevel retryCL;

        private RetryDecision(Type type, ConsistencyLevel retryCL) {
            this.type = type;
            this.retryCL = retryCL;
        }

        /**
         * The type of this retry decision.
         *
         * @return the type of this retry decision.
         */
        public Type getType() {
            return type;
        }

        /**
         * The consistency level for a retry decision.
         *
         * @return the consistency level for a retry decision or {@code null}
         * if this retry decision is an {@code IGNORE} or a {@code RETHROW}.
         */
        public ConsistencyLevel getRetryConsistencyLevel() {
            return retryCL;
        }

        /**
         * Creates a RETHROW retry decision.
         *
         * @return a RETHROW retry decision.
         */
        public static RetryDecision rethrow() {
            return new RetryDecision(Type.RETHROW, null);
        }

        /**
         * Creates a RETRY retry decision using the provided consistency level.
         *
         * @param consistency the consistency level to use for the retry.
         * @return a RETRY with consistency level {@code consistency} retry decision.
         */
        public static RetryDecision retry(ConsistencyLevel consistency) {
            return new RetryDecision(Type.RETRY, consistency);
        }

        /**
         * Creates an IGNORE retry decision.
         *
         * @return an IGNORE retry decision.
         */
        public static RetryDecision ignore() {
            return new RetryDecision(Type.IGNORE, null);
        }
    }

    /**
     * Defines whether to retry and at which consistency level on a read timeout.
     * <p>
     * Note that this method may be called even if
     * {@code requiredResponses >= receivedResponses} if {@code dataPresent} is
     * {@code false} (see
     * {@link com.datastax.driver.core.exceptions.ReadTimeoutException#wasDataRetrieved}).
     *
     * @param cl the original consistency level of the read that timeouted.
     * @param requiredResponses the number of responses that were required to
     * achieve the requested consistency level.
     * @param receivedResponses the number of responses that had been received
     * by the time the timeout exception was raised.
     * @param dataRetrieved whether actual data (by opposition to data checksum)
     * was present in the received responses.
     * @param nbRetry the number of retry already performed for this operation.
     * @return the retry decision. If {@code RetryDecision.RETHROW} is returned,
     * a {@link com.datastax.driver.core.exceptions.ReadTimeoutException} will
     * be thrown for the operation.
     */
    public RetryDecision onReadTimeout(ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry);

    /**
     * Defines whether to retry and at which consistency level on a write timeout.
     *
     * @param cl the original consistency level of the write that timeouted.
     * @param writeType the type of the write that timeouted.
     * @param requiredAcks the number of acknowledgments that were required to
     * achieve the requested consistency level.
     * @param receivedAcks the number of acknowledgments that had been received
     * by the time the timeout exception was raised.
     * @param nbRetry the number of retry already performed for this operation.
     * @return the retry decision. If {@code RetryDecision.RETHROW} is returned,
     * a {@link com.datastax.driver.core.exceptions.WriteTimeoutException} will
     * be thrown for the operation.
     */
    public RetryDecision onWriteTimeout(ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry);

    /**
     * Defines whether to retry and at which consistency level on an
     * unavailable exception.
     *
     * @param cl the original consistency level for the operation.
     * @param requiredReplica the number of replica that should have been
     * (known) alive for the operation to be attempted.
     * @param aliveReplica the number of replica that were know to be alive by
     * the coordinator of the operation.
     * @param nbRetry the number of retry already performed for this operation.
     * @return the retry decision. If {@code RetryDecision.RETHROW} is returned,
     * an {@link com.datastax.driver.core.exceptions.UnavailableException} will
     * be thrown for the operation.
     */
    public RetryDecision onUnavailable(ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry);

    /**
     * The default retry policy.
     * <p>
     * This policy retries queries in only two cases:
     * <ul>
     *   <li>On a read timeout, if enough replica replied but data was not retrieved.</li>
     *   <li>On a write timeout, if we timeout while writting the distributed log used by batch statements.</li>
     * </ul>
     * <p>
     * This retry policy is conservative in that it will never retry with a
     * different consistency level than the one of the initial operation.
     */
    public static class Default implements RetryPolicy {

        public static final Default INSTANCE = new Default();

        private Default() {}

        /**
         * Defines whether to retry and at which consistency level on a read timeout.
         * <p>
         * This method triggers a maximum of one retry, and only if enough
         * replica had responded to the read request but data was not retrieved
         * amongst those. Indeed, that case usually means that enough replica
         * are alive to satisfy the consistency but the coordinator picked a
         * dead one for data retrieval, not having detecte that replica as dead
         * yet. The reasoning for retrying then is that by the time we get the
         * timeout the dead replica will likely have been detected as dead and
         * the retry has a high change of success.
         *
         * @param cl the original consistency level of the read that timeouted.
         * @param requiredResponses the number of responses that were required to
         * achieve the requested consistency level.
         * @param receivedResponses the number of responses that had been received
         * by the time the timeout exception was raised.
         * @param dataRetrieved whether actual data (by opposition to data checksum)
         * was present in the received responses.
         * @param nbRetry the number of retry already performed for this operation.
         * @return {@code RetryDecision.retry(cl)} if no retry attempt has yet been tried and
         * {@code receivedResponses >= requiredResponses && !dataRetrieved}, {@code RetryDecision.rethrow()} otherwise.
         */
        public RetryDecision onReadTimeout(ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
            if (nbRetry != 0)
                return RetryDecision.rethrow();

            return receivedResponses >= requiredResponses && !dataRetrieved ? RetryDecision.retry(cl) : RetryDecision.rethrow();
        }

        /**
         * Defines whether to retry and at which consistency level on a write timeout.
         * <p>
         * This method triggers a maximum of one retry, and only in the case of
         * a {@code WriteType.BATCH_LOG} write. The reasoning for the retry in
         * that case is that write to the distributed batch log is tried by the
         * coordinator of the write against a small subset of all the node alive
         * in the local datacenter. Hence, a timeout usually means that none of
         * the nodes in that subset were alive but the coordinator hasn't
         * detected them as dead. By the time we get the timeout the dead
         * nodes will likely have been detected as dead and the retry has thus a
         * high change of success.
         *
         * @param cl the original consistency level of the write that timeouted.
         * @param writeType the type of the write that timeouted.
         * @param requiredAcks the number of acknowledgments that were required to
         * achieve the requested consistency level.
         * @param receivedAcks the number of acknowledgments that had been received
         * by the time the timeout exception was raised.
         * @param nbRetry the number of retry already performed for this operation.
         * @return {@code RetryDecision.retry(cl)} if no retry attempt has yet been tried and
         * {@code writeType == WriteType.BATCH_LOG}, {@code RetryDecision.rethrow()} otherwise.
         */
        public RetryDecision onWriteTimeout(ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
            if (nbRetry != 0)
                return RetryDecision.rethrow();

            // If the batch log write failed, retry the operation as this might just be we were unlucky at picking candidtes
            return writeType == WriteType.BATCH_LOG ? RetryDecision.retry(cl) : RetryDecision.rethrow();
        }

        /**
         * Defines whether to retry and at which consistency level on an
         * unavailable exception.
         * <p>
         * This method never retries as a retry on an unavailable exception
         * using the same consistency level has almost no change of success.
         *
         * @param cl the original consistency level for the operation.
         * @param requiredReplica the number of replica that should have been
         * (known) alive for the operation to be attempted.
         * @param aliveReplica the number of replica that were know to be alive by
         * the coordinator of the operation.
         * @param nbRetry the number of retry already performed for this operation.
         * @return {@code RetryDecision.rethrow()}.
         */
        public RetryDecision onUnavailable(ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
            return RetryDecision.rethrow();
        }
    }

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
     * policy into {@link RetryPolicy.RetryLogger} to log the occurences of
     * such consistency break.
     * <p>
     * This policy implements the same retries than the {@link Default} policy.
     * But on top of that, it also retries in the following cases:
     * <ul>
     *   <li>On a read timeout: if the number of replica that responded is
     *   greater than one but lower than is required by the requested
     *   consistency level, the operation is retried at a lower concistency
     *   level.</li>
     *   <li>On a write timeout: if the operation is an {@code
     *   WriteType.UNLOGGED_BATCH} and at least one replica acknowleged the
     *   write, the operation is retried at a lower consistency level.
     *   Furthermore, for other operation, if at least one replica acknowleged
     *   the write, the timeout is ignored.</li>
     *   <li>On an unavailable exception: if at least one replica is alive, the
     *   operation is retried at a lower consistency level.</li>
     * </ul>
     * <p>
     * The reasoning behing this retry policy is the following one. If, based
     * on the information the Cassandra coordinator node returns, retrying the
     * operation with the initally requested consistency has a change to
     * succeed, do it. Otherwise, if based on these informations we know <b>the
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
    public static class DowngradingConsistency implements RetryPolicy {

        public static final DowngradingConsistency INSTANCE = new DowngradingConsistency();

        private DowngradingConsistency() {}

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
         * responsed than required by the consistency level (but at least one
         * replica did respond), the operation is retried at a lower
         * consistency level. If enough replica responded but data was not
         * retrieve, the operation is retried with the initial consistency
         * level. Otherwise, an exception is thrown.
         *
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
        public RetryDecision onReadTimeout(ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
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
         * and at least one replica acknowleged, the write is retried with a
         * lower consistency level (with unlogged batch, a write timeout can
         * <b>always</b> mean that part of the batch haven't been persisted at
         * all, even if {@code receivedAcks > 0}). For other {@code writeType},
         * if we know the write has been persisted on at least one replica, we
         * ignore the exception. Otherwise, an exception is thrown.
         *
         * @param cl the original consistency level of the write that timeouted.
         * @param writeType the type of the write that timeouted.
         * @param requiredAcks the number of acknowledgments that were required to
         * achieve the requested consistency level.
         * @param receivedAcks the number of acknowledgments that had been received
         * by the time the timeout exception was raised.
         * @param nbRetry the number of retry already performed for this operation.
         * @return a RetryDecision as defined above.
         */
        public RetryDecision onWriteTimeout(ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
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
         * @param cl the original consistency level for the operation.
         * @param requiredReplica the number of replica that should have been
         * (known) alive for the operation to be attempted.
         * @param aliveReplica the number of replica that were know to be alive by
         * the coordinator of the operation.
         * @param nbRetry the number of retry already performed for this operation.
         * @return a RetryDecision as defined above.
         */
        public RetryDecision onUnavailable(ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
            if (nbRetry != 0)
                return RetryDecision.rethrow();

            // Tries the biggest CL that is expected to work
            return maxLikelyToWorkCL(aliveReplica);
        }
    }

    /**
     * A retry policy that wraps another policy, logging the decision made by its sub-policy.
     * <p>
     * Note that this policy only log the IGNORE and RETRY decisions (since
     * RETHROW decisions just amount to propate the cassandra exception). The
     * logging is done at the INFO level.
     */
    public static class RetryLogger implements RetryPolicy {

        private static final Logger logger = LoggerFactory.getLogger(RetryLogger.class);
        private final RetryPolicy policy;

        private RetryLogger(RetryPolicy policy) {
            this.policy = policy;
        }

        /**
         * Creates a new {@code RetryPolicy} that logs the decision of {@code policy}.
         *
         * @param policy the policy to wrap. The policy created by this method
         * will return the same decision than {@code policy} but will log them.
         * @return the newly create logging policy.
         */
        public static RetryPolicy wrap(RetryPolicy policy) {
            return new RetryLogger(policy);
        }

        private static ConsistencyLevel cl(ConsistencyLevel cl, RetryDecision decision) {
            return decision.retryCL == null ? cl : decision.retryCL;
        }

        public RetryDecision onReadTimeout(ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
            RetryDecision decision = policy.onReadTimeout(cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry);
            switch (decision.type) {
                case IGNORE:
                    String f1 = "Ignoring read timeout (initial consistency: %s, required responses: %i, received responses: %i, data retrieved: %b, retries: %i)";
                    logger.info(String.format(f1, cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry));
                    break;
                case RETRY:
                    String f2 = "Retrying on read timeout at consistency %s (initial consistency: %s, required responses: %i, received responses: %i, data retrieved: %b, retries: %i)";
                    logger.info(String.format(f2, cl(cl, decision), cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry));
                    break;
            }
            return decision;
        }

        public RetryDecision onWriteTimeout(ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
            RetryDecision decision = policy.onWriteTimeout(cl, writeType, requiredAcks, receivedAcks, nbRetry);
            switch (decision.type) {
                case IGNORE:
                    String f1 = "Ignoring write timeout (initial consistency: %s, write type: %s, required acknowledgments: %i, received acknowledgments: %i, retries: %i)";
                    logger.info(String.format(f1, cl, writeType, requiredAcks, receivedAcks, nbRetry));
                    break;
                case RETRY:
                    String f2 = "Retrying on write timeout at consistency %s(initial consistency: %s, write type: %s, required acknowledgments: %i, received acknowledgments: %i, retries: %i)";
                    logger.info(String.format(f2, cl(cl, decision), cl, writeType, requiredAcks, receivedAcks, nbRetry));
                    break;
            }
            return decision;
        }

        public RetryDecision onUnavailable(ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
            RetryDecision decision = policy.onUnavailable(cl, requiredReplica, aliveReplica, nbRetry);
            switch (decision.type) {
                case IGNORE:
                    String f1 = "Ignoring unavailable exception (initial consistency: %s, required replica: %i, alive replica: %i, retries: %i)";
                    logger.info(String.format(f1, cl, requiredReplica, aliveReplica, nbRetry));
                    break;
                case RETRY:
                    String f2 = "Retrying on unavailable exception at consistency %s (initial consistency: %s, required replica: %i, alive replica: %i, retries: %i)";
                    logger.info(String.format(f2, cl(cl, decision), cl, requiredReplica, aliveReplica, nbRetry));
                    break;
            }
            return decision;
        }
    }
}
