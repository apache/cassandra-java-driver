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
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface AlterTableStart
    extends AlterTableWithOptions,
        AlterTableAddColumn,
        AlterTableDropColumn,
        AlterTableRenameColumn {

  /** Completes ALTER TABLE specifying that compact storage should be removed from the table. */
  @NonNull
  BuildableQuery dropCompactStorage();

  /**
   * Completes ALTER TABLE specifying the the type of a column should be changed.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link SchemaBuilder#udt(CqlIdentifier, boolean)}.
   */
  @NonNull
  BuildableQuery alterColumn(@NonNull CqlIdentifier columnName, @NonNull DataType dataType);

  /**
   * Shortcut for {@link #alterColumn(CqlIdentifier,DataType)
   * alterColumn(CqlIdentifier.fromCql(columnName,dataType)}.
   */
  @NonNull
  default BuildableQuery alterColumn(@NonNull String columnName, @NonNull DataType dataType) {
    return alterColumn(CqlIdentifier.fromCql(columnName), dataType);
  }
}
