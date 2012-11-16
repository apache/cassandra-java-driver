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

    /**
     * The constant delay used by this reconnection policy.
     *
     * @return the constant delay used by this reconnection policy.
     */
    public long getConstantDelayMs() {
        return delayMs;
    }

    /**
     * A new schedule that uses a constant {@code getConstantDelayMs()} delay
     * between reconnection attempt.
     *
     * @return the newly created schedule.
     */
    public ReconnectionSchedule newSchedule() {
        return new ConstantSchedule();
    }

    private class ConstantSchedule implements ReconnectionSchedule {

        public long nextDelayMs() {
            return delayMs;
        }
    }
}
