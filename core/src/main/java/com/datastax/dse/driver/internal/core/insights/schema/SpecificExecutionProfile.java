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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SpecificExecutionProfile {
  @JsonProperty("readTimeout")
  private final Integer readTimeout;

  @JsonProperty("loadBalancing")
  private final LoadBalancingInfo loadBalancing;

  @JsonProperty("speculativeExecution")
  private SpeculativeExecutionInfo speculativeExecution;

  @JsonProperty("consistency")
  private final String consistency;

  @JsonProperty("serialConsistency")
  private final String serialConsistency;

  @JsonProperty("graphOptions")
  private Map<String, Object> graphOptions;

  @JsonCreator
  public SpecificExecutionProfile(
      @JsonProperty("readTimeout") Integer readTimeoutMillis,
      @JsonProperty("loadBalancing") LoadBalancingInfo loadBalancing,
      @JsonProperty("speculativeExecution") SpeculativeExecutionInfo speculativeExecutionInfo,
      @JsonProperty("consistency") String consistency,
      @JsonProperty("serialConsistency") String serialConsistency,
      @JsonProperty("graphOptions") Map<String, Object> graphOptions) {
    readTimeout = readTimeoutMillis;
    this.loadBalancing = loadBalancing;
    this.speculativeExecution = speculativeExecutionInfo;
    this.consistency = consistency;
    this.serialConsistency = serialConsistency;
    this.graphOptions = graphOptions;
  }

  public Integer getReadTimeout() {
    return readTimeout;
  }

  public LoadBalancingInfo getLoadBalancing() {
    return loadBalancing;
  }

  public SpeculativeExecutionInfo getSpeculativeExecution() {
    return speculativeExecution;
  }

  public String getConsistency() {
    return consistency;
  }

  public String getSerialConsistency() {
    return serialConsistency;
  }

  public Map<String, Object> getGraphOptions() {
    return graphOptions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SpecificExecutionProfile)) {
      return false;
    }
    SpecificExecutionProfile that = (SpecificExecutionProfile) o;
    return Objects.equals(readTimeout, that.readTimeout)
        && Objects.equals(loadBalancing, that.loadBalancing)
        && Objects.equals(speculativeExecution, that.speculativeExecution)
        && Objects.equals(consistency, that.consistency)
        && Objects.equals(serialConsistency, that.serialConsistency)
        && Objects.equals(graphOptions, that.graphOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        readTimeout,
        loadBalancing,
        speculativeExecution,
        consistency,
        serialConsistency,
        graphOptions);
  }

  @Override
  public String toString() {
    return "SpecificExecutionProfile{"
        + "readTimeout="
        + readTimeout
        + ", loadBalancing="
        + loadBalancing
        + ", speculativeExecution="
        + speculativeExecution
        + ", consistency='"
        + consistency
        + '\''
        + ", serialConsistency='"
        + serialConsistency
        + '\''
        + ", graphOptions="
        + graphOptions
        + '}';
  }
}
