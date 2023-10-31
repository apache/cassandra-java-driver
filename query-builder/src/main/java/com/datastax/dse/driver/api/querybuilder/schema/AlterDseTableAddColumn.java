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
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import javax.annotation.Nonnull;

public interface AlterDseTableAddColumn {
  /**
   * Adds a column definition in the ALTER TABLE statement.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link SchemaBuilder#udt(CqlIdentifier, boolean)}.
   */
  @Nonnull
  AlterDseTableAddColumnEnd addColumn(
      @Nonnull CqlIdentifier columnName, @Nonnull DataType dataType);

  /**
   * Shortcut for {@link #addColumn(CqlIdentifier, DataType)
   * addColumn(CqlIdentifier.asCql(columnName), dataType)}.
   */
  @Nonnull
  default AlterDseTableAddColumnEnd addColumn(
      @Nonnull String columnName, @Nonnull DataType dataType) {
    return addColumn(CqlIdentifier.fromCql(columnName), dataType);
  }

  /**
   * Adds a static column definition in the ALTER TABLE statement.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link SchemaBuilder#udt(CqlIdentifier, boolean)}.
   */
  @Nonnull
  AlterDseTableAddColumnEnd addStaticColumn(
      @Nonnull CqlIdentifier columnName, @Nonnull DataType dataType);

  /**
   * Shortcut for {@link #addStaticColumn(CqlIdentifier, DataType)
   * addStaticColumn(CqlIdentifier.asCql(columnName), dataType)}.
   */
  @Nonnull
  default AlterDseTableAddColumnEnd addStaticColumn(
      @Nonnull String columnName, @Nonnull DataType dataType) {
    return addStaticColumn(CqlIdentifier.fromCql(columnName), dataType);
  }
}
