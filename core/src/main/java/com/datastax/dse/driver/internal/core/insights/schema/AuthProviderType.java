/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
