/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
