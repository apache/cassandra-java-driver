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
package com.datastax.oss.driver.api.core.type;

import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

/**
 * Type representing a Cassandra vector type as described in CEP-30.  At the moment
 * this is implemented as a custom type so we include the CustomType interface as well.
 */
public class VectorType implements CustomType {

  public static final String VECTOR_CLASS_NAME = "org.apache.cassandra.db.marshal.VectorType";

  private final DataType subtype;
  private final int dimensions;

  public VectorType(DataType subtype, int dimensions) {

    this.dimensions = dimensions;
    this.subtype = subtype;
  }

  public int getDimensions() {
    return this.dimensions;
  }

  public DataType getSubtype() {
    return this.subtype;
  }

  @NonNull
  @Override
  public String getClassName() {
    return VECTOR_CLASS_NAME;
  }

  @NonNull
  @Override
  public String asCql(boolean includeFrozen, boolean pretty) {
    return String.format("'%s(%d)'", getClassName(), getDimensions());
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o instanceof VectorType) {
      VectorType that = (VectorType) o;
      return that.subtype.equals(this.subtype) && that.dimensions == this.dimensions;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), subtype, dimensions);
  }

  @Override
  public String toString() {
    return String.format("CqlVector(%s, %d)", getSubtype(), getDimensions());
  }

  @Override
  public boolean isDetached() {
    return false;
  }

  @Override
  public void attach(@NonNull AttachmentPoint attachmentPoint) {
    // nothing to do
  }
}
