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
package com.datastax.dse.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface AlterDseTableDropColumn {
  /**
   * Adds column(s) to drop to ALTER TABLE specification. This may be repeated with successive calls
   * to drop columns.
   */
  @NonNull
  AlterDseTableDropColumnEnd dropColumns(@NonNull CqlIdentifier... columnNames);

  /** Shortcut for {@link #dropColumns(CqlIdentifier...)}. */
  @NonNull
  default AlterDseTableDropColumnEnd dropColumns(@NonNull String... columnNames) {
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
  @NonNull
  default AlterDseTableDropColumnEnd dropColumn(@NonNull CqlIdentifier columnName) {
    return dropColumns(columnName);
  }

  /**
   * Shortcut for {@link #dropColumn(CqlIdentifier) dropColumn(CqlIdentifier.fromCql(columnName))}.
   */
  @NonNull
  default AlterDseTableDropColumnEnd dropColumn(@NonNull String columnName) {
    return dropColumns(CqlIdentifier.fromCql(columnName));
  }
}
