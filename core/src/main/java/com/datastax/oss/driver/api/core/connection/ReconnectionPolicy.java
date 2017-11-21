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

import java.time.Duration;

/**
 * Decides how often the driver tries to re-establish lost connections to a node.
 *
 * <p>Each time a connection to a node is lost, a {@link #newSchedule() new schedule} instance gets
 * created. Then {@link ReconnectionSchedule#nextDelay()} will be called each time the driver needs
 * to schedule the next connection attempt. When the node is back to its required number of
 * connections, the schedule will be reset (that is, the next failure will create a fresh schedule
 * instance).
 */
public interface ReconnectionPolicy extends AutoCloseable {

  /** Creates a new schedule. */
  ReconnectionSchedule newSchedule();

  /** Called when the cluster that this policy is associated with closes. */
  @Override
  void close();

  /**
   * The reconnection schedule from the time a connection is lost, to the time all connections to
   * this node have been restored.
   */
  interface ReconnectionSchedule {
    /** How long to wait before the next reconnection attempt. */
    Duration nextDelay();
  }
}
