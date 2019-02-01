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
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reconnection policy that waits exponentially longer between each reconnection attempt (but
 * keeps a constant delay once a maximum delay is reached).
 *
 * <p>It uses the same schedule implementation for individual nodes or the control connection:
 * reconnection attempt {@code i} will be tried {@code Math.min(2^(i-1) * getBaseDelayMs(),
 * getMaxDelayMs())} milliseconds after the previous one. A random amount of jitter (+/- 15%) will
 * be added to the pure exponential delay value to avoid situations where many clients are in the
 * reconnection process at exactly the same time. The jitter will never cause the delay to be less
 * than the base delay, or more than the max delay.
 *
 * <p>To activate this policy, modify the {@code advanced.reconnection-policy} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.reconnection-policy {
 *     class = ExponentialReconnectionPolicy
 *     base-delay = 1 second
 *     max-delay = 60 seconds
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
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
    this.logPrefix = context.getSessionName();

    DriverExecutionProfile config = context.getConfig().getDefaultProfile();

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
  public ReconnectionSchedule newControlConnectionSchedule(
      @SuppressWarnings("ignored") boolean isInitialConnection) {
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
      long delay = (attempts > maxAttempts) ? maxDelayMs : calculateDelayWithJitter();
      return Duration.ofMillis(delay);
    }

    private long calculateDelayWithJitter() {
      // assert we haven't hit the max attempts
      assert attempts <= maxAttempts;
      // get the pure exponential delay based on the attempt count
      long delay = Math.min(baseDelayMs * (1L << attempts++), maxDelayMs);
      // calculate up to 15% jitter, plus or minus (i.e. 85 - 115% of the pure value)
      int jitter = ThreadLocalRandom.current().nextInt(85, 116);
      // apply jitter
      delay = (jitter * delay) / 100;
      // ensure the final delay is between the base and max
      delay = Math.min(maxDelayMs, Math.max(baseDelayMs, delay));
      return delay;
    }
  }

  public long getMaxAttempts() {
    return maxAttempts;
  }
}
