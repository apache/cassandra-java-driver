package com.datastax.driver.core.policies;

/**
 * A reconnection policy that waits exponentially longer between each
 * reconnection attempt (but keeps a constant delay once a maximum delay is
 * reached).
 */
public class ExponentialReconnectionPolicy implements ReconnectionPolicy {

    private final long baseDelayMs;
    private final long maxDelayMs;

    /**
     * Creates a reconnection policy waiting exponentially longer for each new attempt.
     *
     * @param baseDelayMs the base delay in milliseconds to use for
     * the schedules created by this policy. 
     * @param maxDelayMs the maximum delay to wait between two attempts.
     */
    public ExponentialReconnectionPolicy(long baseDelayMs, long maxDelayMs) {
        if (baseDelayMs < 0 || maxDelayMs < 0)
            throw new IllegalArgumentException("Invalid negative delay");
        if (maxDelayMs < baseDelayMs)
            throw new IllegalArgumentException(String.format("maxDelayMs (got %d) cannot be smaller than baseDelayMs (got %d)", maxDelayMs, baseDelayMs));

        this.baseDelayMs = baseDelayMs;
        this.maxDelayMs = maxDelayMs;
    }

    public ExponentialSchedule newSchedule() {
        return new ExponentialSchedule();
    }

    public class ExponentialSchedule implements ReconnectionSchedule {

        private int attempts;

        /**
         * The delay before the next reconnection.
         * <p>
         * For this schedule, reconnection attempt {@code i} will be tried
         * {@code 2^i * baseDelayMs} milliseconds after the previous one
         * (unless {@code maxDelayMs} has been reached, in which case all
         * following attempts will be done with a delay of {@code maxDelayMs}),
         * where {@code baseDelayMs} (and {@code maxDelayMs}) are the
         * delays sets by the {@code ExponentialReconnectionPolicy} from
         * which this schedule has been created.
         *
         * @return the delay before the next reconnection.
         */
        public long nextDelayMs() {
            // We "overflow" at 64 attempts but I doubt this matter
            if (attempts >= 64)
                return maxDelayMs;

            long next = baseDelayMs * (1L << attempts++);
            return next > maxDelayMs ? maxDelayMs : next;
        }
    }
}
