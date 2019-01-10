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

public interface AlterDseTableRenameColumn {

  /**
   * Adds a column rename to ALTER TABLE specification. This may be repeated with successive calls
   * to rename columns.
   */
  @NonNull
  AlterDseTableRenameColumnEnd renameColumn(@NonNull CqlIdentifier from, @NonNull CqlIdentifier to);

  /**
   * Shortcut for {@link #renameColumn(CqlIdentifier, CqlIdentifier)
   * renameField(CqlIdentifier.fromCql(from),CqlIdentifier.fromCql(to))}.
   */
  @NonNull
  default AlterDseTableRenameColumnEnd renameColumn(@NonNull String from, @NonNull String to) {
    return renameColumn(CqlIdentifier.fromCql(from), CqlIdentifier.fromCql(to));
  }
}
