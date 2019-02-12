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

public interface AlterTypeStart extends AlterTypeRenameField {

  /**
   * Completes ALTER TYPE specifying the the type of a field should be changed.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link SchemaBuilder#udt(CqlIdentifier, boolean)}.
   */
  @NonNull
  BuildableQuery alterField(@NonNull CqlIdentifier fieldName, @NonNull DataType dataType);

  /**
   * Shortcut for {@link #alterField(CqlIdentifier,DataType)
   * alterField(CqlIdentifier.fromCql(columnName,dataType)}.
   */
  @NonNull
  default BuildableQuery alterField(@NonNull String fieldName, @NonNull DataType dataType) {
    return alterField(CqlIdentifier.fromCql(fieldName), dataType);
  }

  /**
   * Completes ALTER TYPE by adding a field definition in the ALTER TYPE statement.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link SchemaBuilder#udt(CqlIdentifier, boolean)}.
   */
  @NonNull
  BuildableQuery addField(@NonNull CqlIdentifier fieldName, @NonNull DataType dataType);

  /**
   * Shortcut for {@link #addField(CqlIdentifier, DataType) addField(CqlIdentifier.asCql(fieldName),
   * dataType)}.
   */
  @NonNull
  default BuildableQuery addField(@NonNull String fieldName, @NonNull DataType dataType) {
    return addField(CqlIdentifier.fromCql(fieldName), dataType);
  }
}
