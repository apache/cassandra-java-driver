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
import com.datastax.oss.driver.api.querybuilder.SchemaBuilderDsl;

public interface CreateTable extends BuildableQuery, OngoingPartitionKey, CreateTableWithOptions {

  /**
   * Adds a clustering column definition in the CREATE TABLE statement.
   *
   * <p>This includes the column declaration (you don't need an additional {@link
   * #withColumn(CqlIdentifier, DataType) addColumn} call).
   *
   * <p>Clustering key columns are added in the order of their declaration.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link SchemaBuilderDsl#udt(CqlIdentifier, boolean)}.
   */
  CreateTable withClusteringColumn(CqlIdentifier columnName, DataType dataType);

  /**
   * Shortcut for {@link #withClusteringColumn(CqlIdentifier, DataType)
   * withClusteringColumn(CqlIdentifier.asCql(columnName), dataType)}.
   */
  default CreateTable withClusteringColumn(String columnName, DataType dataType) {
    return withClusteringColumn(CqlIdentifier.fromCql(columnName), dataType);
  }

  /**
   * Adds a column definition in the CREATE TABLE statement.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link SchemaBuilderDsl#udt(CqlIdentifier, boolean)}.
   */
  CreateTable withColumn(CqlIdentifier columnName, DataType dataType);

  /**
   * Shortcut for {@link #withColumn(CqlIdentifier, DataType)
   * withColumn(CqlIdentifier.asCql(columnName), dataType)}.
   */
  default CreateTable withColumn(String columnName, DataType dataType) {
    return withColumn(CqlIdentifier.fromCql(columnName), dataType);
  }

  /**
   * Adds a static column definition in the CREATE TABLE statement.
   *
   * <p>This includes the column declaration (you don't need an additional {@link
   * #withColumn(CqlIdentifier, DataType) addColumn} call).
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link SchemaBuilderDsl#udt(CqlIdentifier, boolean)}.
   */
  CreateTable withStaticColumn(CqlIdentifier columnName, DataType dataType);

  /**
   * Shortcut for {@link #withStaticColumn(CqlIdentifier, DataType)
   * withStaticColumn(CqlIdentifier.asCql(columnName), dataType)}.
   */
  default CreateTable withStaticColumn(String columnName, DataType dataType) {
    return withStaticColumn(CqlIdentifier.fromCql(columnName), dataType);
  }
}
