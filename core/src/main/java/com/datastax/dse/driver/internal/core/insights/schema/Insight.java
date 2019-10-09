/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
