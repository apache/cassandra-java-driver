/*
 *      Copyright (C) 2012 DataStax Inc.
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

    /**
     * The base delay in milliseconds for this policy (e.g. the delay before
     * the first reconnection attempt).
     *
     * @return the base delay in milliseconds for this policy.
     */
    public long getBaseDelayMs() {
        return baseDelayMs;
    }

    /**
     * The maximum delay in milliseconds between reconnection attempts for this policy.
     *
     * @return the maximum delay in milliseconds between reconnection attempts for this policy.
     */
    public long getMaxDelayMs() {
        return maxDelayMs;
    }

    /**
     * A new schedule that used an exponentially growing delay between reconnection attempts.
     * <p>
     * For this schedule, reconnection attempt {@code i} will be tried
     * {@code Math.min(2^(i-1) * getBaseDelayMs(), getMaxDelayMs())} milliseconds after the previous one.
     *
     * @return the newly created schedule.
     */
    public ReconnectionSchedule newSchedule() {
        return new ExponentialSchedule();
    }

    private class ExponentialSchedule implements ReconnectionSchedule {

        private int attempts;

        public long nextDelayMs() {
            // We "overflow" at 64 attempts but I doubt this matter
            if (attempts >= 64)
                return maxDelayMs;

            return Math.min(baseDelayMs * (1L << attempts++), maxDelayMs);
        }
    }
}
