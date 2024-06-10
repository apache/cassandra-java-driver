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
import java.util.Map;
import java.util.Objects;

public class InsightsStatusData {
  @JsonProperty("clientId")
  private final String clientId;

  @JsonProperty("sessionId")
  private final String sessionId;

  @JsonProperty("controlConnection")
  private final String controlConnection;

  @JsonProperty("connectedNodes")
  private final Map<String, SessionStateForNode> connectedNodes;

  @JsonCreator
  private InsightsStatusData(
      @JsonProperty("clientId") String clientId,
      @JsonProperty("sessionId") String sessionId,
      @JsonProperty("controlConnection") String controlConnection,
      @JsonProperty("connectedNodes") Map<String, SessionStateForNode> connectedNodes) {
    this.clientId = clientId;
    this.sessionId = sessionId;
    this.controlConnection = controlConnection;
    this.connectedNodes = connectedNodes;
  }

  public String getClientId() {
    return clientId;
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getControlConnection() {
    return controlConnection;
  }

  public Map<String, SessionStateForNode> getConnectedNodes() {
    return connectedNodes;
  }

  @Override
  public String toString() {
    return "InsightsStatusData{"
        + "clientId='"
        + clientId
        + '\''
        + ", sessionId='"
        + sessionId
        + '\''
        + ", controlConnection="
        + controlConnection
        + ", connectedNodes="
        + connectedNodes
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InsightsStatusData)) {
      return false;
    }
    InsightsStatusData that = (InsightsStatusData) o;
    return Objects.equals(clientId, that.clientId)
        && Objects.equals(sessionId, that.sessionId)
        && Objects.equals(controlConnection, that.controlConnection)
        && Objects.equals(connectedNodes, that.connectedNodes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clientId, sessionId, controlConnection, connectedNodes);
  }

  public static InsightsStatusData.Builder builder() {
    return new InsightsStatusData.Builder();
  }

  public static class Builder {
    private String clientId;
    private String sessionId;
    private String controlConnection;
    private Map<String, SessionStateForNode> connectedNodes;

    public Builder withClientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder withSessionId(String id) {
      this.sessionId = id;
      return this;
    }

    public Builder withControlConnection(String controlConnection) {
      this.controlConnection = controlConnection;
      return this;
    }

    public Builder withConnectedNodes(Map<String, SessionStateForNode> connectedNodes) {
      this.connectedNodes = connectedNodes;
      return this;
    }

    public InsightsStatusData build() {
      return new InsightsStatusData(clientId, sessionId, controlConnection, connectedNodes);
    }
  }
}
