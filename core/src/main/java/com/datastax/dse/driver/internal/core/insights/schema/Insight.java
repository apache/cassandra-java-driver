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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Insight<T> {
  @JsonProperty("metadata")
  private final InsightMetadata metadata;

  @JsonProperty("data")
  private final T insightData;

  @JsonCreator
  public Insight(@JsonProperty("metadata") InsightMetadata metadata, @JsonProperty("data") T data) {
    this.metadata = metadata;
    this.insightData = data;
  }

  public InsightMetadata getMetadata() {
    return metadata;
  }

  public T getInsightData() {
    return insightData;
  }

  @Override
  public String toString() {
    return "Insight{" + "metadata=" + metadata + ", insightData=" + insightData + '}';
  }
}
