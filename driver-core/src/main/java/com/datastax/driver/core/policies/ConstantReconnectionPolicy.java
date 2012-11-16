package com.datastax.driver.core.policies;

/**
 * A reconnection policy that waits a constant time between each reconnection attempt.
 */
public class ConstantReconnectionPolicy implements ReconnectionPolicy {

    private final long delayMs;

    /**
     * Creates a reconnection policy that creates with the provided constant wait
     * time between reconnection attempts.
     *
     * @param constantDelayMs the constant delay in milliseconds to use.
     */
    public ConstantReconnectionPolicy(long constantDelayMs) {
        if (constantDelayMs < 0)
            throw new IllegalArgumentException(String.format("Invalid negative delay (got %d)", constantDelayMs));

        this.delayMs = constantDelayMs;
    }

    public ConstantSchedule newSchedule() {
        return new ConstantSchedule();
    }

    public class ConstantSchedule implements ReconnectionSchedule {

        /**
         * The delay before the next reconnection.
         *
         * @return the fixed delay set by the {@code
         * ConstantReconnectionPolicy} that created this schedule.
         */
        public long nextDelayMs() {
            return delayMs;
        }
    }
}
