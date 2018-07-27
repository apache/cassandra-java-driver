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
package com.datastax.oss.driver.internal.core.specex;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.ThreadSafe;

/**
 * A policy that schedules a configurable number of speculative executions, separated by a fixed
 * delay.
 *
 * <p>To activate this policy, modify the {@code advanced.speculative-execution-policy} section in
 * the driver configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.speculative-execution-policy {
 *     class = ConstantSpeculativeExecutionPolicy
 *     max-executions = 3
 *     delay = 100 milliseconds
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
@ThreadSafe
public class ConstantSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {

  private final int maxExecutions;
  private final long constantDelayMillis;

  public ConstantSpeculativeExecutionPolicy(DriverContext context, String profileName) {
    DriverExecutionProfile config = context.getConfig().getProfile(profileName);
    this.maxExecutions = config.getInt(DefaultDriverOption.SPECULATIVE_EXECUTION_MAX);
    if (this.maxExecutions < 1) {
      throw new IllegalArgumentException("Max must be at least 1");
    }
    this.constantDelayMillis =
        config.getDuration(DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY).toMillis();
    if (this.constantDelayMillis < 0) {
      throw new IllegalArgumentException("Delay must be positive or 0");
    }
  }

  @Override
  public long nextExecution(
      @NonNull @SuppressWarnings("unused") Node node,
      @Nullable @SuppressWarnings("unused") CqlIdentifier keyspace,
      @NonNull @SuppressWarnings("unused") Request request,
      int runningExecutions) {
    assert runningExecutions >= 1;
    return (runningExecutions < maxExecutions) ? constantDelayMillis : -1;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
