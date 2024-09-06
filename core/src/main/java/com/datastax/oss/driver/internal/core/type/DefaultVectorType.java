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
package com.datastax.oss.driver.internal.core.type;

import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.VectorType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultVectorType implements VectorType {

  public static final String VECTOR_CLASS_NAME = "org.apache.cassandra.db.marshal.VectorType";

  private final DataType subtype;
  private final int dimensions;

  public DefaultVectorType(DataType subtype, int dimensions) {

    this.dimensions = dimensions;
    this.subtype = subtype;
  }

  /* ============== ContainerType interface ============== */
  @Override
  public DataType getElementType() {
    return this.subtype;
  }

  /* ============== VectorType interface ============== */
  @Override
  public int getDimensions() {
    return this.dimensions;
  }

  /* ============== CustomType interface ============== */
  @NonNull
  @Override
  public String getClassName() {
    return VECTOR_CLASS_NAME;
  }

  @NonNull
  @Override
  public String asCql(boolean includeFrozen, boolean pretty) {
    return String.format(
        "vector<%s, %d>", this.subtype.asCql(includeFrozen, pretty).toLowerCase(), getDimensions());
  }

  /* ============== General class implementation ============== */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o instanceof DefaultVectorType) {
      DefaultVectorType that = (DefaultVectorType) o;
      return that.subtype.equals(this.subtype) && that.dimensions == this.dimensions;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(DefaultVectorType.class, subtype, dimensions);
  }

  @Override
  public String toString() {
    return String.format("Vector(%s, %d)", getElementType(), getDimensions());
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
