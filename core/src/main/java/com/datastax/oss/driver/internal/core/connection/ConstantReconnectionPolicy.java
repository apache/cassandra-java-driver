/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reconnection policy that waits a constant time between each reconnection attempt.
 *
 * <p>To activate this policy, modify the {@code advanced.reconnection-policy} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.reconnection-policy {
 *     class = ConstantReconnectionPolicy
 *     base-delay = 1 second
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
public class ConstantReconnectionPolicy implements ReconnectionPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(ConstantReconnectionPolicy.class);

  private final String logPrefix;
  private final ReconnectionSchedule schedule;

  /** Builds a new instance. */
  public ConstantReconnectionPolicy(DriverContext context) {
    this.logPrefix = context.getSessionName();
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    Duration delay = config.getDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY);
    if (delay.isNegative()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid negative delay for "
                  + DefaultDriverOption.RECONNECTION_BASE_DELAY.getPath()
                  + " (got %d)",
              delay));
    }
    this.schedule = () -> delay;
  }

  @NonNull
  @Override
  public ReconnectionSchedule newNodeSchedule(@NonNull Node node) {
    LOG.debug("[{}] Creating new schedule for {}", logPrefix, node);
    return schedule;
  }

  @NonNull
  @Override
  public ReconnectionSchedule newControlConnectionSchedule(
      @SuppressWarnings("ignored") boolean isInitialConnection) {
    LOG.debug("[{}] Creating new schedule for the control connection", logPrefix);
    return schedule;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
