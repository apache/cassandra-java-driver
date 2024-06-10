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

public class SSL {
  @JsonProperty("enabled")
  private final boolean enabled;

  @JsonProperty("certValidation")
  private final boolean certValidation;

  @JsonCreator
  public SSL(
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("certValidation") boolean certValidation) {
    this.enabled = enabled;
    this.certValidation = certValidation;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public boolean isCertValidation() {
    return certValidation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SSL)) {
      return false;
    }
    SSL that = (SSL) o;
    return enabled == that.enabled && certValidation == that.certValidation;
  }

  @Override
  public int hashCode() {
    return Objects.hash(enabled, certValidation);
  }
}
