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
package com.datastax.dse.driver.internal.core.insights.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class SessionStateForNode {
  @JsonProperty("connections")
  private final Integer connections;

  @JsonProperty("inFlightQueries")
  private final Integer inFlightQueries;

  @JsonCreator
  public SessionStateForNode(
      @JsonProperty("connections") Integer connections,
      @JsonProperty("inFlightQueries") Integer inFlightQueries) {
    this.connections = connections;
    this.inFlightQueries = inFlightQueries;
  }

  public Integer getConnections() {
    return connections;
  }

  public Integer getInFlightQueries() {
    return inFlightQueries;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SessionStateForNode)) {
      return false;
    }
    SessionStateForNode that = (SessionStateForNode) o;
    return Objects.equals(connections, that.connections)
        && Objects.equals(inFlightQueries, that.inFlightQueries);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connections, inFlightQueries);
  }

  @Override
  public String toString() {
    return "SessionStateForNode{"
        + "connections="
        + connections
        + ", inFlightQueries="
        + inFlightQueries
        + '}';
  }
}
