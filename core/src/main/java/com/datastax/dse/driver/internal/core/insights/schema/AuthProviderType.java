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

public class AuthProviderType {
  @JsonProperty("type")
  private final String type;

  @JsonProperty("namespace")
  private final String namespace;

  @JsonCreator
  public AuthProviderType(
      @JsonProperty("type") String type, @JsonProperty("namespace") String namespace) {
    this.type = type;
    this.namespace = namespace;
  }

  public String getType() {
    return type;
  }

  public String getNamespace() {
    return namespace;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AuthProviderType)) {
      return false;
    }
    AuthProviderType that = (AuthProviderType) o;
    return Objects.equals(type, that.type) && Objects.equals(namespace, that.namespace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, namespace);
  }

  @Override
  public String toString() {
    return "AuthProviderType{" + "type='" + type + '\'' + ", namespace='" + namespace + '\'' + '}';
  }
}
