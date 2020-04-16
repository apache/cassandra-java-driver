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
import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultIndexMetadata implements IndexMetadata, Serializable {

  private static final long serialVersionUID = 1;

  @NonNull private final CqlIdentifier keyspace;
  @NonNull private final CqlIdentifier table;
  @NonNull private final CqlIdentifier name;
  @NonNull private final IndexKind kind;
  @NonNull private final String target;
  @NonNull private final Map<String, String> options;

  public DefaultIndexMetadata(
      @NonNull CqlIdentifier keyspace,
      @NonNull CqlIdentifier table,
      @NonNull CqlIdentifier name,
      @NonNull IndexKind kind,
      @NonNull String target,
      @NonNull Map<String, String> options) {
    this.keyspace = keyspace;
    this.table = table;
    this.name = name;
    this.kind = kind;
    this.target = target;
    this.options = options;
  }

  @NonNull
  @Override
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NonNull
  @Override
  public CqlIdentifier getTable() {
    return table;
  }

  @NonNull
  @Override
  public CqlIdentifier getName() {
    return name;
  }

  @NonNull
  @Override
  public IndexKind getKind() {
    return kind;
  }

  @NonNull
  @Override
  public String getTarget() {
    return target;
  }

  @NonNull
  @Override
  public Map<String, String> getOptions() {
    return options;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof IndexMetadata) {
      IndexMetadata that = (IndexMetadata) other;
      return Objects.equals(this.keyspace, that.getKeyspace())
          && Objects.equals(this.table, that.getTable())
          && Objects.equals(this.name, that.getName())
          && Objects.equals(this.kind, that.getKind())
          && Objects.equals(this.target, that.getTarget())
          && Objects.equals(this.options, that.getOptions());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyspace, table, name, kind, target, options);
  }

  @Override
  public String toString() {
    return "DefaultIndexMetadata@"
        + Integer.toHexString(hashCode())
        + "("
        + keyspace.asInternal()
        + "."
        + table.asInternal()
        + "."
        + name.asInternal()
        + ")";
  }
}
