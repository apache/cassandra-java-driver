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
package com.datastax.oss.driver.internal.core.connection;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reconnection policy that waits exponentially longer between each reconnection attempt (but
 * keeps a constant delay once a maximum delay is reached).
 *
 * <p>It uses the same schedule implementation for individual nodes or the control connection:
 * reconnection attempt {@code i} will be tried {@code Math.min(2^(i-1) * getBaseDelayMs(),
 * getMaxDelayMs())} milliseconds after the previous one.
 */
@ThreadSafe
public class ExponentialReconnectionPolicy implements ReconnectionPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(ExponentialReconnectionPolicy.class);

  private final String logPrefix;
  private final long baseDelayMs;
  private final long maxDelayMs;
  private final long maxAttempts;

  /** Builds a new instance. */
  public ExponentialReconnectionPolicy(DriverContext context) {
    this.logPrefix = context.sessionName();

    DriverConfigProfile config = context.config().getDefaultProfile();

    this.baseDelayMs = config.getDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY).toMillis();
    this.maxDelayMs = config.getDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY).toMillis();

    Preconditions.checkArgument(
        baseDelayMs > 0,
        "%s must be strictly positive (got %s)",
        DefaultDriverOption.RECONNECTION_BASE_DELAY.getPath(),
        baseDelayMs);
    Preconditions.checkArgument(
        maxDelayMs >= 0,
        "%s must be positive (got %s)",
        DefaultDriverOption.RECONNECTION_MAX_DELAY.getPath(),
        maxDelayMs);
    Preconditions.checkArgument(
        maxDelayMs >= baseDelayMs,
        "%s must be bigger than %s (got %s, %s)",
        DefaultDriverOption.RECONNECTION_MAX_DELAY.getPath(),
        DefaultDriverOption.RECONNECTION_BASE_DELAY.getPath(),
        maxDelayMs,
        baseDelayMs);

    // Maximum number of attempts after which we overflow
    int ceil = (baseDelayMs & (baseDelayMs - 1)) == 0 ? 0 : 1;
    this.maxAttempts = 64L - Long.numberOfLeadingZeros(Long.MAX_VALUE / baseDelayMs) - ceil;
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

  @NonNull
  @Override
  public ReconnectionSchedule newNodeSchedule(@NonNull Node node) {
    LOG.debug("[{}] Creating new schedule for {}", logPrefix, node);
    return new ExponentialSchedule();
  }

  @NonNull
  @Override
  public ReconnectionSchedule newControlConnectionSchedule() {
    LOG.debug("[{}] Creating new schedule for the control connection", logPrefix);
    return new ExponentialSchedule();
  }

  @Override
  public void close() {
    // nothing to do
  }

  private class ExponentialSchedule implements ReconnectionSchedule {

    private int attempts;

    @NonNull
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
