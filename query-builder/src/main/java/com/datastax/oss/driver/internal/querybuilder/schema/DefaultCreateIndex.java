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
package com.datastax.oss.driver.internal.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndex;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndexOnTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndexStart;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultCreateIndex implements CreateIndexStart, CreateIndexOnTable, CreateIndex {

  private static final String NO_INDEX_TYPE = "__NO_INDEX_TYPE";

  private final CqlIdentifier indexName;

  private final boolean ifNotExists;

  private final CqlIdentifier keyspace;
  private final CqlIdentifier table;

  private final ImmutableMap<CqlIdentifier, String> columnToIndexType;

  private final String usingClass;

  private final ImmutableMap<String, Object> options;

  public DefaultCreateIndex() {
    this(null);
  }

  public DefaultCreateIndex(@Nullable CqlIdentifier indexName) {
    this(indexName, false, null, null, ImmutableMap.of(), null, ImmutableMap.of());
  }

  public DefaultCreateIndex(
      @Nullable CqlIdentifier indexName,
      boolean ifNotExists,
      @Nullable CqlIdentifier keyspace,
      @Nullable CqlIdentifier table,
      @NonNull ImmutableMap<CqlIdentifier, String> columnToIndexType,
      @Nullable String usingClass,
      @NonNull ImmutableMap<String, Object> options) {
    this.indexName = indexName;
    this.ifNotExists = ifNotExists;
    this.keyspace = keyspace;
    this.table = table;
    this.columnToIndexType = columnToIndexType;
    this.usingClass = usingClass;
    this.options = options;
  }

  @NonNull
  @Override
  public CreateIndex andColumn(@NonNull CqlIdentifier column, @Nullable String indexType) {
    // use placeholder index type when none present as immutable map does not allow null values.
    if (indexType == null) {
      indexType = NO_INDEX_TYPE;
    }

    return new DefaultCreateIndex(
        indexName,
        ifNotExists,
        keyspace,
        table,
        ImmutableCollections.append(columnToIndexType, column, indexType),
        usingClass,
        options);
  }

  @NonNull
  @Override
  public CreateIndexStart ifNotExists() {
    return new DefaultCreateIndex(
        indexName, true, keyspace, table, columnToIndexType, usingClass, options);
  }

  @NonNull
  @Override
  public CreateIndexStart custom(@NonNull String className) {
    return new DefaultCreateIndex(
        indexName, ifNotExists, keyspace, table, columnToIndexType, className, options);
  }

  @NonNull
  @Override
  public CreateIndexOnTable onTable(CqlIdentifier keyspace, @NonNull CqlIdentifier table) {
    return new DefaultCreateIndex(
        indexName, ifNotExists, keyspace, table, columnToIndexType, usingClass, options);
  }

  @NonNull
  @Override
  public CreateIndex withOption(@NonNull String name, @NonNull Object value) {
    return new DefaultCreateIndex(
        indexName,
        ifNotExists,
        keyspace,
        table,
        columnToIndexType,
        usingClass,
        ImmutableCollections.append(options, name, value));
  }

  @NonNull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder("CREATE ");
    if (usingClass != null) {
      builder.append("CUSTOM ");
    }
    builder.append("INDEX");
    if (ifNotExists) {
      builder.append(" IF NOT EXISTS");
    }

    if (indexName != null) {
      builder.append(' ').append(indexName.asCql(true));
    }

    if (table == null) {
      // Table not provided yet.
      return builder.toString();
    }

    builder.append(" ON ");

    CqlHelper.qualify(keyspace, table, builder);

    if (columnToIndexType.isEmpty()) {
      // columns not provided yet
      return builder.toString();
    }

    builder.append(" (");

    boolean firstColumn = true;
    for (Map.Entry<CqlIdentifier, String> entry : columnToIndexType.entrySet()) {
      if (firstColumn) {
        firstColumn = false;
      } else {
        builder.append(",");
      }
      if (entry.getValue().equals(NO_INDEX_TYPE)) {
        builder.append(entry.getKey());
      } else {
        builder.append(entry.getValue()).append("(").append(entry.getKey()).append(")");
      }
    }
    builder.append(")");

    if (usingClass != null) {
      builder.append(" USING '").append(usingClass).append('\'');
    }

    if (!options.isEmpty()) {
      builder.append(OptionsUtils.buildOptions(options, true));
    }

    return builder.toString();
  }

  @Override
  public String toString() {
    return asCql();
  }

  @NonNull
  @Override
  public Map<String, Object> getOptions() {
    return options;
  }

  @Nullable
  public CqlIdentifier getIndex() {
    return indexName;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  @Nullable
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @Nullable
  public CqlIdentifier getTable() {
    return table;
  }

  @NonNull
  public ImmutableMap<CqlIdentifier, String> getColumnToIndexType() {
    return columnToIndexType;
  }

  @Nullable
  public String getUsingClass() {
    return usingClass;
  }
}
