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
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import javax.annotation.Nonnull;

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
   * {@link SchemaBuilder#udt(CqlIdentifier, boolean)}.
   */
  @Nonnull
  CreateTable withClusteringColumn(@Nonnull CqlIdentifier columnName, @Nonnull DataType dataType);

  /**
   * Shortcut for {@link #withClusteringColumn(CqlIdentifier, DataType)
   * withClusteringColumn(CqlIdentifier.asCql(columnName), dataType)}.
   */
  @Nonnull
  default CreateTable withClusteringColumn(@Nonnull String columnName, @Nonnull DataType dataType) {
    return withClusteringColumn(CqlIdentifier.fromCql(columnName), dataType);
  }

  /**
   * Adds a column definition in the CREATE TABLE statement.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link SchemaBuilder#udt(CqlIdentifier, boolean)}.
   */
  @Nonnull
  CreateTable withColumn(@Nonnull CqlIdentifier columnName, @Nonnull DataType dataType);

  /**
   * Shortcut for {@link #withColumn(CqlIdentifier, DataType)
   * withColumn(CqlIdentifier.asCql(columnName), dataType)}.
   */
  @Nonnull
  default CreateTable withColumn(@Nonnull String columnName, @Nonnull DataType dataType) {
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
  @Nonnull
  CreateTable withStaticColumn(@Nonnull CqlIdentifier columnName, @Nonnull DataType dataType);

  /**
   * Shortcut for {@link #withStaticColumn(CqlIdentifier, DataType)
   * withStaticColumn(CqlIdentifier.asCql(columnName), dataType)}.
   */
  @Nonnull
  default CreateTable withStaticColumn(@Nonnull String columnName, @Nonnull DataType dataType) {
    return withStaticColumn(CqlIdentifier.fromCql(columnName), dataType);
  }
}
