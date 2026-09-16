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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.metadata.Node;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * Indicates that a node announced a graceful shutdown (CEP-59): a {@code GRACEFUL_DISCONNECT}
 * protocol event was received on one of its connections.
 */
@Immutable
public class GracefulDisconnectEvent {

  /** The node that is shutting down. */
  public final Node node;

  public GracefulDisconnectEvent(Node node) {
    this.node = node;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof GracefulDisconnectEvent) {
      GracefulDisconnectEvent that = (GracefulDisconnectEvent) other;
      return Objects.equals(this.node, that.node);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.node);
  }

  @Override
  public String toString() {
    return "GracefulDisconnectEvent(" + node + ")";
  }
}
