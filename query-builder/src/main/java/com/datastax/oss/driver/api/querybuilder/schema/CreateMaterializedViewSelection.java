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
import com.google.common.collect.Iterables;
import java.util.Arrays;

public interface CreateMaterializedViewSelection {

  /** Selects all columns from the base table. */
  CreateMaterializedViewWhereStart all();

  /** Selects a particular column by its CQL identifier. */
  CreateMaterializedViewSelectionWithColumns column(CqlIdentifier columnName);

  /** Shortcut for {@link #column(CqlIdentifier) column(CqlIdentifier.fromCql(columnName))} */
  default CreateMaterializedViewSelectionWithColumns column(String columnName) {
    return column(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Convenience method to select multiple simple columns at once, as in {@code SELECT a,b,c}.
   *
   * <p>This is the same as calling {@link #column(CqlIdentifier)} for each element.
   */
  CreateMaterializedViewSelectionWithColumns columnsIds(Iterable<CqlIdentifier> columnIds);

  /** Var-arg equivalent of {@link #columnsIds(Iterable)}. */
  default CreateMaterializedViewSelectionWithColumns columns(CqlIdentifier... columnIds) {
    return columnsIds(Arrays.asList(columnIds));
  }

  /**
   * Convenience method to select multiple simple columns at once, as in {@code SELECT a,b,c}.
   *
   * <p>This is the same as calling {@link #column(String)} for each element.
   */
  default CreateMaterializedViewSelectionWithColumns columns(Iterable<String> columnNames) {
    return columnsIds(Iterables.transform(columnNames, CqlIdentifier::fromInternal));
  }

  /** Var-arg equivalent of {@link #columns(Iterable)}. */
  default CreateMaterializedViewSelectionWithColumns columns(String... columnNames) {
    return columns(Arrays.asList(columnNames));
  }
}
