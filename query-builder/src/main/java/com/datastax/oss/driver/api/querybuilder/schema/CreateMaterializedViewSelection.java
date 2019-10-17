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
package com.datastax.oss.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.CqlIdentifiers;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;

public interface CreateMaterializedViewSelection {

  /** Selects all columns from the base table. */
  @NonNull
  CreateMaterializedViewWhereStart all();

  /** Selects a particular column by its CQL identifier. */
  @NonNull
  CreateMaterializedViewSelectionWithColumns column(@NonNull CqlIdentifier columnName);

  /** Shortcut for {@link #column(CqlIdentifier) column(CqlIdentifier.fromCql(columnName))} */
  @NonNull
  default CreateMaterializedViewSelectionWithColumns column(@NonNull String columnName) {
    return column(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Convenience method to select multiple simple columns at once, as in {@code SELECT a,b,c}.
   *
   * <p>This is the same as calling {@link #column(CqlIdentifier)} for each element.
   */
  @NonNull
  CreateMaterializedViewSelectionWithColumns columnsIds(@NonNull Iterable<CqlIdentifier> columnIds);

  /** Var-arg equivalent of {@link #columnsIds(Iterable)}. */
  @NonNull
  default CreateMaterializedViewSelectionWithColumns columns(@NonNull CqlIdentifier... columnIds) {
    return columnsIds(Arrays.asList(columnIds));
  }

  /**
   * Convenience method to select multiple simple columns at once, as in {@code SELECT a,b,c}.
   *
   * <p>This is the same as calling {@link #column(String)} for each element.
   */
  @NonNull
  default CreateMaterializedViewSelectionWithColumns columns(
      @NonNull Iterable<String> columnNames) {
    return columnsIds(CqlIdentifiers.wrap(columnNames));
  }

  /** Var-arg equivalent of {@link #columns(Iterable)}. */
  @NonNull
  default CreateMaterializedViewSelectionWithColumns columns(@NonNull String... columnNames) {
    return columns(Arrays.asList(columnNames));
  }
}
