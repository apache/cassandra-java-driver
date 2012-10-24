package com.datastax.driver.core.configuration;

/**
 * Policy that decides how often the reconnection to a dead node is attempted.
 *
 * Each time a node is detected dead (because a connection error occurs), a new
 * {@code ReconnectionPolicy} instance is created (based on which {@link
 * ReconnectionPolicy.Factory} has been configured). Then each call to the
 * {@link #nextDelayMs} method of this instance will decide when the next
 * reconnection attempt to this node will be tried.
 *
 * Note that independently of the reconnection policy, the driver will attempt
 * a reconnection if it received a push notification from the Cassandra cluster
 * that the node is UP again. So this reconnection policy is mainly useful in
 * case where the client loose connection to a node without that node actually
 * being down.
 *
 * The default {@link ReconnectionPolicy.Exponential} policy is usually
 * adequate.
 */
public interface ReconnectionPolicy {

    /**
     * When to attempt the next reconnection. 
     *
     * This method will be called once when the host is detected down to
     * schedul the first reconnection attempt, and then once after each failed
     * reconnection attempt to schedule the next one. Hence each call to this
     * method are free to return a different value.
     *
     * @return a time in milliseconds to wait before attempting the next
     * reconnection.
     */
    public long nextDelayMs();

    /**
     * Simple factory interface to create {@link ReconnectionPolicy} instances.
     */
    public interface Factory {

        /**
         * Creates a new connection policy instance.
         *
         * @return a new {@code ReconnectionPolicy} instance.
         */
        public ReconnectionPolicy create();
    }

    /**
     * A reconnection policy that waits a constant time between each reconnection attempt.
     */
    public static class Constant implements ReconnectionPolicy {

        private final long delayMs;

        // TODO: validate arguments
        private Constant(long delayMs) {
            this.delayMs = delayMs;
        }

        public long nextDelayMs() {
            return delayMs;
        }

        /**
         * Creates a reconnection policy factory that creates {@link
         * ReconnectionPolicy.Constant} policies with the provided constant wait
         * time.
         *
         * @param constantDelayMs the constant delay in milliseconds to use for
         * the reconnection policy created by the factory returned by this
         * method.
         * @return a reconnection policy factory that creates {@code
         * Reconnection.Constant} policies with a {@code constantDelayMs}
         * milliseconds delay between reconnection attempts.
         */
        public static ReconnectionPolicy.Factory makeFactory(final long constantDelayMs) {
            return new ReconnectionPolicy.Factory() {
                public ReconnectionPolicy create() {
                    return new Constant(constantDelayMs);
                }
            };
        }
    }

    /**
     * A reconnection policy that waits exponentially longer between each
     * reconnection attempt (but keeps a constant delay once a maximum delay is
     * reached).
     */
    public static class Exponential implements ReconnectionPolicy {

        private final long baseDelayMs;
        private final long maxDelayMs;
        private int attempts;

        // TODO: validate arguments
        private Exponential(long baseDelayMs, long maxDelayMs) {
            this.baseDelayMs = baseDelayMs;
            this.maxDelayMs = maxDelayMs;
        }

        public long nextDelayMs() {
            ++attempts;
            return baseDelayMs * (1 << attempts);
        }

        /**
         * Creates a reconnection policy factory that creates {@link
         * ReconnectionPolicy.Exponential} policies with the provided base and
         * max delays.
         *
         * @param baseDelayMs the base delay in milliseconds to use for
         * the reconnection policy created by the factory returned by this
         * method. Reconnection attempt {@code i} will be tried
         * {@code 2^i * baseDelayMs} milliseconds after the previous one
         * (unless {@code maxDelayMs} has been reached, in which case all
         * following attempts will be done with a delay of {@code maxDelayMs}).
         * @param maxDelayMs the maximum delay to wait between two attempts.
         * @return a reconnection policy factory that creates {@code
         * Reconnection.Constant} policies with a {@code constantDelayMs}
         * milliseconds delay between reconnection attempts.
         */
        public static ReconnectionPolicy.Factory makeFactory(final long baseDelayMs, final long maxDelayMs) {
            return new ReconnectionPolicy.Factory() {
                public ReconnectionPolicy create() {
                    return new Exponential(baseDelayMs, maxDelayMs);
                }
            };
        }
    }
}
