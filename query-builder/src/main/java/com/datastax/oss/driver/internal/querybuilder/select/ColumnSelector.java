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
package com.datastax.oss.driver.internal.querybuilder.select;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class ColumnSelector implements Selector {

  private final CqlIdentifier columnId;
  private final CqlIdentifier alias;

  public ColumnSelector(@NonNull CqlIdentifier columnId) {
    this(columnId, null);
  }

  public ColumnSelector(@NonNull CqlIdentifier columnId, @Nullable CqlIdentifier alias) {
    Preconditions.checkNotNull(columnId);
    this.columnId = columnId;
    this.alias = alias;
  }

  @NonNull
  @Override
  public Selector as(@NonNull CqlIdentifier alias) {
    return new ColumnSelector(columnId, alias);
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    builder.append(columnId.asCql(true));
    if (alias != null) {
      builder.append(" AS ").append(alias.asCql(true));
    }
  }

  @NonNull
  public CqlIdentifier getColumnId() {
    return columnId;
  }

  @Nullable
  @Override
  public CqlIdentifier getAlias() {
    return alias;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof ColumnSelector) {
      ColumnSelector that = (ColumnSelector) other;
      return this.columnId.equals(that.columnId) && Objects.equals(this.alias, that.alias);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnId, alias);
  }
}
