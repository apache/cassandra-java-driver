/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.metadata.schema;

import com.datastax.dse.driver.api.core.metadata.schema.DseVertexMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

public class DefaultDseVertexMetadata implements DseVertexMetadata {

  @NonNull private final CqlIdentifier labelName;

  public DefaultDseVertexMetadata(@NonNull CqlIdentifier labelName) {
    this.labelName = Preconditions.checkNotNull(labelName);
  }

  @NonNull
  @Override
  public CqlIdentifier getLabelName() {
    return labelName;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DefaultDseVertexMetadata) {
      DefaultDseVertexMetadata that = (DefaultDseVertexMetadata) other;
      return Objects.equals(this.labelName, that.getLabelName());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return labelName.hashCode();
  }
}
