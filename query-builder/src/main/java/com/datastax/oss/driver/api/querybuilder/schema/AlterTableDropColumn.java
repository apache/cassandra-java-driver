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
import javax.annotation.Nonnull;

public interface AlterTableDropColumn {
  /**
   * Adds column(s) to drop to ALTER TABLE specification. This may be repeated with successive calls
   * to drop columns.
   */
  @Nonnull
  AlterTableDropColumnEnd dropColumns(@Nonnull CqlIdentifier... columnNames);

  /** Shortcut for {@link #dropColumns(CqlIdentifier...)}. */
  @Nonnull
  default AlterTableDropColumnEnd dropColumns(@Nonnull String... columnNames) {
    CqlIdentifier ids[] = new CqlIdentifier[columnNames.length];
    for (int i = 0; i < columnNames.length; i++) {
      ids[i] = CqlIdentifier.fromCql(columnNames[i]);
    }
    return dropColumns(ids);
  }

  /**
   * Adds a column to drop to ALTER TABLE specification. This may be repeated with successive calls
   * to drop columns. Shortcut for {@link #dropColumns(CqlIdentifier...) #dropColumns(columnName)}.
   */
  @Nonnull
  default AlterTableDropColumnEnd dropColumn(@Nonnull CqlIdentifier columnName) {
    return dropColumns(columnName);
  }

  /**
   * Shortcut for {@link #dropColumn(CqlIdentifier) dropColumn(CqlIdentifier.fromCql(columnName))}.
   */
  @Nonnull
  default AlterTableDropColumnEnd dropColumn(@Nonnull String columnName) {
    return dropColumns(CqlIdentifier.fromCql(columnName));
  }
}
