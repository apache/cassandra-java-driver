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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.metadata.Node;
import net.jcip.annotations.Immutable;

/**
 * This event indicates that the server is shutting down gracefully and the driver should:
 *
 * <ul>
 *   <li>Stop sending new requests on the affected connection
 *   <li>Allow in-flight requests to complete
 *   <li>Begin reconnection attempts with exponential backoff
 * </ul>
 *
 * <p>This is part of CEP-59: Graceful Disconnect – In-Band Connection Draining for Node Shutdown.
 */
@Immutable
public class GracefulDisconnectEvent {

  /** The event type string as defined in the native protocol. */
  public static final String EVENT_TYPE = "GRACEFUL_DISCONNECT";

  /** The node that sent the graceful disconnect event. */
  public final Node node;

  /** The channel that received the graceful disconnect event. */
  public final DriverChannel channel;

  public GracefulDisconnectEvent(Node node, DriverChannel channel) {
    this.node = node;
    this.channel = channel;
  }

  @Override
  public String toString() {
    return "GracefulDisconnectEvent{node=" + node + ", channel=" + channel + '}';
  }
}
