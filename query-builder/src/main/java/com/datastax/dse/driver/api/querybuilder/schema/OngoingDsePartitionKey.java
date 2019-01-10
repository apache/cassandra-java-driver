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
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface OngoingDsePartitionKey {

  /**
   * Adds a partition key column definition.
   *
   * <p>This includes the column declaration (you don't need an additional {@link
   * CreateTable#withColumn(CqlIdentifier, DataType) addColumn} call).
   *
   * <p>Partition keys are added in the order of their declaration.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link SchemaBuilder#udt(CqlIdentifier, boolean)}.
   */
  @NonNull
  CreateDseTable withPartitionKey(@NonNull CqlIdentifier columnName, @NonNull DataType dataType);

  /**
   * Shortcut for {@link #withPartitionKey(CqlIdentifier, DataType)
   * withPartitionKey(CqlIdentifier.asCql(columnName), dataType)}.
   */
  @NonNull
  default CreateDseTable withPartitionKey(@NonNull String columnName, @NonNull DataType dataType) {
    return withPartitionKey(CqlIdentifier.fromCql(columnName), dataType);
  }
}
