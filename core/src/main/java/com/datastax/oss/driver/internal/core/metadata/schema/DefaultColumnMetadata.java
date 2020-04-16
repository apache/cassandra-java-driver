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
package com.datastax.oss.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.Serializable;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultColumnMetadata implements ColumnMetadata, Serializable {

  private static final long serialVersionUID = 1;

  @NonNull private final CqlIdentifier keyspace;
  @NonNull private final CqlIdentifier parent;
  @NonNull private final CqlIdentifier name;
  @NonNull private final DataType dataType;
  private final boolean isStatic;

  public DefaultColumnMetadata(
      @NonNull CqlIdentifier keyspace,
      @NonNull CqlIdentifier parent,
      @NonNull CqlIdentifier name,
      @NonNull DataType dataType,
      boolean isStatic) {
    this.keyspace = keyspace;
    this.parent = parent;
    this.name = name;
    this.dataType = dataType;
    this.isStatic = isStatic;
  }

  @NonNull
  @Override
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NonNull
  @Override
  public CqlIdentifier getParent() {
    return parent;
  }

  @NonNull
  @Override
  public CqlIdentifier getName() {
    return name;
  }

  @NonNull
  @Override
  public DataType getType() {
    return dataType;
  }

  @Override
  public boolean isStatic() {
    return isStatic;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof ColumnMetadata) {
      ColumnMetadata that = (ColumnMetadata) other;
      return Objects.equals(this.keyspace, that.getKeyspace())
          && Objects.equals(this.parent, that.getParent())
          && Objects.equals(this.name, that.getName())
          && Objects.equals(this.dataType, that.getType())
          && this.isStatic == that.isStatic();
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyspace, parent, name, dataType, isStatic);
  }

  @Override
  public String toString() {
    return "DefaultColumnMetadata@"
        + Integer.toHexString(hashCode())
        + "("
        + keyspace.asInternal()
        + "."
        + parent.asInternal()
        + "."
        + name.asInternal()
        + " "
        + dataType.asCql(true, false)
        + ")";
  }
}
