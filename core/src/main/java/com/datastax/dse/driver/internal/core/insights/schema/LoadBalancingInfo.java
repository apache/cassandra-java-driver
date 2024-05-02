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

public class LoadBalancingInfo {
  @JsonProperty("type")
  private final String type;

  @JsonProperty("options")
  private final Map<String, Object> options;

  @JsonProperty("namespace")
  private final String namespace;

  @JsonCreator
  public LoadBalancingInfo(
      @JsonProperty("type") String type,
      @JsonProperty("options") Map<String, Object> options,
      @JsonProperty("namespace") String namespace) {
    this.type = type;
    this.options = options;
    this.namespace = namespace;
  }

  public String getType() {
    return type;
  }

  public Map<String, Object> getOptions() {
    return options;
  }

  public String getNamespace() {
    return namespace;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LoadBalancingInfo)) {
      return false;
    }
    LoadBalancingInfo that = (LoadBalancingInfo) o;
    return Objects.equals(type, that.type)
        && Objects.equals(options, that.options)
        && Objects.equals(namespace, that.namespace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, options, namespace);
  }

  @Override
  public String toString() {
    return "LoadBalancingInfo{"
        + "type='"
        + type
        + '\''
        + ", options="
        + options
        + ", namespace='"
        + namespace
        + '\''
        + '}';
  }
}
