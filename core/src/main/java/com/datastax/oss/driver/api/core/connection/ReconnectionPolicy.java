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
package com.datastax.oss.driver.api.core.connection;

import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;

/**
 * Decides how often the driver tries to re-establish lost connections.
 *
 * <p>When a reconnection starts, the driver invokes this policy to create a {@link
 * ReconnectionSchedule ReconnectionSchedule} instance. That schedule's {@link
 * ReconnectionSchedule#nextDelay() nextDelay()} method will get called each time the driver needs
 * to program the next connection attempt. When the reconnection succeeds, the schedule is
 * discarded; if the connection is lost again later, the next reconnection attempt will query the
 * policy again to obtain a new schedule.
 *
 * <p>There are two types of reconnection:
 *
 * <ul>
 *   <li>{@linkplain #newNodeSchedule(Node) for regular node connections}: when the connection pool
 *       for a node does not have its configured number of connections (see {@code
 *       advanced.connection.pool.*.size} in the configuration), a reconnection starts for that
 *       pool.
 *   <li>{@linkplain #newControlConnectionSchedule(boolean) for the control connection}: when the
 *       control node goes down, a reconnection starts to find another node to replace it. This is
 *       also used if the configuration option {@code advanced.reconnect-on-init} is set and the
 *       driver has to retry the initial connection.
 * </ul>
 *
 * This interface defines separate methods for those two cases, but implementations are free to
 * delegate to the same method internally if the same type of schedule can be used.
 */
public interface ReconnectionPolicy extends AutoCloseable {

  /** Creates a new schedule for the given node. */
  @NonNull
  ReconnectionSchedule newNodeSchedule(@NonNull Node node);

  /**
   * Creates a new schedule for the control connection.
   *
   * @param isInitialConnection whether this schedule is generated for the driver's initial attempt
   *     to connect to the cluster.
   *     <ul>
   *       <li>{@code true} means that the configuration option {@code advanced.reconnect-on-init}
   *           is set, the driver failed to reach any contact point, and it is now scheduling
   *           reattempts.
   *       <li>{@code false} means that the driver was already initialized, lost connection to the
   *           control node, and is now scheduling attempts to connect to another node.
   *     </ul>
   */
  @NonNull
  ReconnectionSchedule newControlConnectionSchedule(boolean isInitialConnection);

  /** Called when the cluster that this policy is associated with closes. */
  @Override
  void close();

  /**
   * The reconnection schedule from the time a connection is lost, to the time all connections to
   * this node have been restored.
   */
  interface ReconnectionSchedule {
    /** How long to wait before the next reconnection attempt. */
    @NonNull
    Duration nextDelay();
  }
}
