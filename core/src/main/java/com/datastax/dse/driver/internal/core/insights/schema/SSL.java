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
