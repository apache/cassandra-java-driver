/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.insights.schema;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;

public class InsightMetadata {
  @JsonProperty("name")
  private final String name;

  @JsonProperty("timestamp")
  private final long timestamp;

  @JsonProperty("tags")
  private final Map<String, String> tags;

  @JsonProperty("insightType")
  private final InsightType insightType;

  @JsonProperty("insightMappingId")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String insightMappingId;

  @JsonCreator
  public InsightMetadata(
      @JsonProperty("name") String name,
      @JsonProperty("timestamp") long timestamp,
      @JsonProperty("tags") Map<String, String> tags,
      @JsonProperty("insightType") InsightType insightType,
      @JsonProperty("insightMappingId") String insightMappingId) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "name is required");

    this.name = name;
    this.timestamp = timestamp;
    this.tags = tags;
    this.insightType = insightType;
    this.insightMappingId = insightMappingId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InsightMetadata)) {
      return false;
    }
    InsightMetadata that = (InsightMetadata) o;
    return Objects.equals(name, that.name)
        && Objects.equals(timestamp, that.timestamp)
        && Objects.equals(tags, that.tags)
        && insightType == that.insightType
        && Objects.equals(insightMappingId, that.insightMappingId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, timestamp, tags, insightType, insightMappingId);
  }

  @Override
  public String toString() {
    return "InsightMetadata{"
        + "name='"
        + name
        + '\''
        + ", timestamp="
        + timestamp
        + ", tags="
        + tags
        + ", insightType="
        + insightType
        + ", insightMappingId="
        + insightMappingId
        + '}';
  }

  public String getName() {
    return name;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public InsightType getInsightType() {
    return insightType;
  }

  public String getInsightMappingId() {
    return insightMappingId;
  }
}
