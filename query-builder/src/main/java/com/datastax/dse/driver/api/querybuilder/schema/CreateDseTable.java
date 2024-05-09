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
package com.datastax.dse.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface CreateDseTable
    extends BuildableQuery, OngoingDsePartitionKey, CreateDseTableWithOptions {

  /**
   * Adds a clustering column definition in the CREATE TABLE statement.
   *
   * <p>This includes the column declaration (you don't need an additional {@link
   * #withColumn(CqlIdentifier, DataType) addColumn} call).
   *
   * <p>Clustering key columns are added in the order of their declaration.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link SchemaBuilder#udt(CqlIdentifier, boolean)}.
   */
  @NonNull
  CreateDseTable withClusteringColumn(
      @NonNull CqlIdentifier columnName, @NonNull DataType dataType);

  /**
   * Shortcut for {@link #withClusteringColumn(CqlIdentifier, DataType)
   * withClusteringColumn(CqlIdentifier.asCql(columnName), dataType)}.
   */
  @NonNull
  default CreateDseTable withClusteringColumn(
      @NonNull String columnName, @NonNull DataType dataType) {
    return withClusteringColumn(CqlIdentifier.fromCql(columnName), dataType);
  }

  /**
   * Adds a column definition in the CREATE TABLE statement.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link SchemaBuilder#udt(CqlIdentifier, boolean)}.
   */
  @NonNull
  CreateDseTable withColumn(@NonNull CqlIdentifier columnName, @NonNull DataType dataType);

  /**
   * Shortcut for {@link #withColumn(CqlIdentifier, DataType)
   * withColumn(CqlIdentifier.asCql(columnName), dataType)}.
   */
  @NonNull
  default CreateDseTable withColumn(@NonNull String columnName, @NonNull DataType dataType) {
    return withColumn(CqlIdentifier.fromCql(columnName), dataType);
  }

  /**
   * Adds a static column definition in the CREATE TABLE statement.
   *
   * <p>This includes the column declaration (you don't need an additional {@link
   * #withColumn(CqlIdentifier, DataType) addColumn} call).
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link SchemaBuilder#udt(CqlIdentifier, boolean)}.
   */
  @NonNull
  CreateDseTable withStaticColumn(@NonNull CqlIdentifier columnName, @NonNull DataType dataType);

  /**
   * Shortcut for {@link #withStaticColumn(CqlIdentifier, DataType)
   * withStaticColumn(CqlIdentifier.asCql(columnName), dataType)}.
   */
  @NonNull
  default CreateDseTable withStaticColumn(@NonNull String columnName, @NonNull DataType dataType) {
    return withStaticColumn(CqlIdentifier.fromCql(columnName), dataType);
  }
}
