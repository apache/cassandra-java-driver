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
package com.datastax.oss.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.CqlIdentifiers;
import java.util.Arrays;
import javax.annotation.Nonnull;

public interface CreateMaterializedViewSelection {

  /** Selects all columns from the base table. */
  @Nonnull
  CreateMaterializedViewWhereStart all();

  /** Selects a particular column by its CQL identifier. */
  @Nonnull
  CreateMaterializedViewSelectionWithColumns column(@Nonnull CqlIdentifier columnName);

  /** Shortcut for {@link #column(CqlIdentifier) column(CqlIdentifier.fromCql(columnName))} */
  @Nonnull
  default CreateMaterializedViewSelectionWithColumns column(@Nonnull String columnName) {
    return column(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Convenience method to select multiple simple columns at once, as in {@code SELECT a,b,c}.
   *
   * <p>This is the same as calling {@link #column(CqlIdentifier)} for each element.
   */
  @Nonnull
  CreateMaterializedViewSelectionWithColumns columnsIds(@Nonnull Iterable<CqlIdentifier> columnIds);

  /** Var-arg equivalent of {@link #columnsIds(Iterable)}. */
  @Nonnull
  default CreateMaterializedViewSelectionWithColumns columns(@Nonnull CqlIdentifier... columnIds) {
    return columnsIds(Arrays.asList(columnIds));
  }

  /**
   * Convenience method to select multiple simple columns at once, as in {@code SELECT a,b,c}.
   *
   * <p>This is the same as calling {@link #column(String)} for each element.
   */
  @Nonnull
  default CreateMaterializedViewSelectionWithColumns columns(
      @Nonnull Iterable<String> columnNames) {
    return columnsIds(CqlIdentifiers.wrap(columnNames));
  }

  /** Var-arg equivalent of {@link #columns(Iterable)}. */
  @Nonnull
  default CreateMaterializedViewSelectionWithColumns columns(@Nonnull String... columnNames) {
    return columns(Arrays.asList(columnNames));
  }
}
