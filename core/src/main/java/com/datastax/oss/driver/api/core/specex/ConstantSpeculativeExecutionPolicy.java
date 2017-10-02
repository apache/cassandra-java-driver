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
package com.datastax.oss.driver.api.core.specex;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.Request;

/**
 * A policy that schedules a configurable number of speculative executions, separated by a fixed
 * delay.
 *
 * <p>See the (commented) sample configuration in {@code reference.conf} for detailed explanations
 * about each option.
 */
public class ConstantSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {

  private final int maxExecutions;
  private final long constantDelayMillis;

  public ConstantSpeculativeExecutionPolicy(DriverContext context) {
    DriverConfigProfile config = context.config().getDefaultProfile();
    this.maxExecutions = config.getInt(CoreDriverOption.SPECULATIVE_EXECUTION_MAX);
    if (this.maxExecutions < 1) {
      throw new IllegalArgumentException("Max must be at least 1");
    }
    this.constantDelayMillis =
        config.getDuration(CoreDriverOption.SPECULATIVE_EXECUTION_DELAY).toMillis();
    if (this.constantDelayMillis < 0) {
      throw new IllegalArgumentException("Delay must be positive or 0");
    }
  }

  @Override
  public long nextExecution(CqlIdentifier keyspace, Request request, int runningExecutions) {
    assert runningExecutions >= 1;
    return (runningExecutions < maxExecutions) ? constantDelayMillis : -1;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
