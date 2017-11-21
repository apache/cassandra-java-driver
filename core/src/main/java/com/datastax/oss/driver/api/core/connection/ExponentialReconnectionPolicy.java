/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.api.core.connection;

import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.google.common.base.Preconditions;
import java.time.Duration;

/**
 * A reconnection policy that waits exponentially longer between each reconnection attempt (but
 * keeps a constant delay once a maximum delay is reached).
 */
public class ExponentialReconnectionPolicy implements ReconnectionPolicy {

  private final long baseDelayMs;
  private final long maxDelayMs;
  private final long maxAttempts;

  /** Builds a new instance. */
  public ExponentialReconnectionPolicy(DriverContext context, DriverOption configRoot) {
    DriverConfigProfile config = context.config().getDefaultProfile();
    DriverOption baseDelayOption =
        configRoot.concat(CoreDriverOption.RELATIVE_EXPONENTIAL_RECONNECTION_BASE_DELAY);
    DriverOption maxDelayOption =
        configRoot.concat(CoreDriverOption.RELATIVE_EXPONENTIAL_RECONNECTION_MAX_DELAY);

    this.baseDelayMs = config.getDuration(baseDelayOption).toMillis();
    this.maxDelayMs = config.getDuration(maxDelayOption).toMillis();

    Preconditions.checkArgument(
        baseDelayMs > 0,
        "%s must be strictly positive (got %s)",
        baseDelayOption.getPath(),
        baseDelayMs);
    Preconditions.checkArgument(
        maxDelayMs >= 0, "%s must be positive (got %s)", maxDelayOption.getPath(), maxDelayMs);
    Preconditions.checkArgument(
        maxDelayMs >= baseDelayMs,
        "%s must be bigger than %s (got %s, %s)",
        maxDelayOption.getPath(),
        baseDelayOption.getPath(),
        maxDelayMs,
        baseDelayMs);

    // Maximum number of attempts after which we overflow
    int ceil = (baseDelayMs & (baseDelayMs - 1)) == 0 ? 0 : 1;
    this.maxAttempts = 64 - Long.numberOfLeadingZeros(Long.MAX_VALUE / baseDelayMs) - ceil;
  }

  /**
   * The base delay in milliseconds for this policy (e.g. the delay before the first reconnection
   * attempt).
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
   *
   * <p>For this schedule, reconnection attempt {@code i} will be tried {@code Math.min(2^(i-1) *
   * getBaseDelayMs(), getMaxDelayMs())} milliseconds after the previous one.
   *
   * @return the newly created schedule.
   */
  @Override
  public ReconnectionSchedule newSchedule() {
    return new ExponentialSchedule();
  }

  @Override
  public void close() {
    // nothing to do
  }

  private class ExponentialSchedule implements ReconnectionSchedule {

    private int attempts;

    @Override
    public Duration nextDelay() {
      long delay =
          (attempts > maxAttempts)
              ? maxDelayMs
              : Math.min(baseDelayMs * (1L << attempts++), maxDelayMs);
      return Duration.ofMillis(delay);
    }
  }
}
