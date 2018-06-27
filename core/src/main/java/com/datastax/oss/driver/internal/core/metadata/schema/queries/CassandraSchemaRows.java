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
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeClassNameParser;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeCqlNameParser;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeParser;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableListMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import net.jcip.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class CassandraSchemaRows implements SchemaRows {

  private final DataTypeParser dataTypeParser;
  private final CompletableFuture<Metadata> refreshFuture;
  private final List<AdminRow> keyspaces;
  private final Multimap<CqlIdentifier, AdminRow> tables;
  private final Multimap<CqlIdentifier, AdminRow> views;
  private final Multimap<CqlIdentifier, AdminRow> types;
  private final Multimap<CqlIdentifier, AdminRow> functions;
  private final Multimap<CqlIdentifier, AdminRow> aggregates;
  private final Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> columns;
  private final Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> indexes;

  private CassandraSchemaRows(
      boolean isCassandraV3,
      CompletableFuture<Metadata> refreshFuture,
      List<AdminRow> keyspaces,
      Multimap<CqlIdentifier, AdminRow> tables,
      Multimap<CqlIdentifier, AdminRow> views,
      Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> columns,
      Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> indexes,
      Multimap<CqlIdentifier, AdminRow> types,
      Multimap<CqlIdentifier, AdminRow> functions,
      Multimap<CqlIdentifier, AdminRow> aggregates) {
    this.dataTypeParser =
        isCassandraV3 ? new DataTypeCqlNameParser() : new DataTypeClassNameParser();
    this.refreshFuture = refreshFuture;
    this.keyspaces = keyspaces;
    this.tables = tables;
    this.views = views;
    this.columns = columns;
    this.indexes = indexes;
    this.types = types;
    this.functions = functions;
    this.aggregates = aggregates;
  }

  @Override
  public DataTypeParser dataTypeParser() {
    return dataTypeParser;
  }

  @Override
  public CompletableFuture<Metadata> refreshFuture() {
    return refreshFuture;
  }

  @Override
  public List<AdminRow> keyspaces() {
    return keyspaces;
  }

  @Override
  public Multimap<CqlIdentifier, AdminRow> tables() {
    return tables;
  }

  @Override
  public Multimap<CqlIdentifier, AdminRow> views() {
    return views;
  }

  @Override
  public Multimap<CqlIdentifier, AdminRow> types() {
    return types;
  }

  @Override
  public Multimap<CqlIdentifier, AdminRow> functions() {
    return functions;
  }

  @Override
  public Multimap<CqlIdentifier, AdminRow> aggregates() {
    return aggregates;
  }

  @Override
  public Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> columns() {
    return columns;
  }

  @Override
  public Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> indexes() {
    return indexes;
  }

  public static class Builder {
    private static final Logger LOG = LoggerFactory.getLogger(Builder.class);

    private final boolean isCassandraV3;
    private final CompletableFuture<Metadata> refreshFuture;
    private final String tableNameColumn;
    private final String logPrefix;
    private final ImmutableList.Builder<AdminRow> keyspacesBuilder = ImmutableList.builder();
    private final ImmutableMultimap.Builder<CqlIdentifier, AdminRow> tablesBuilder =
        ImmutableListMultimap.builder();
    private final ImmutableMultimap.Builder<CqlIdentifier, AdminRow> viewsBuilder =
        ImmutableListMultimap.builder();
    private final ImmutableMultimap.Builder<CqlIdentifier, AdminRow> typesBuilder =
        ImmutableListMultimap.builder();
    private final ImmutableMultimap.Builder<CqlIdentifier, AdminRow> functionsBuilder =
        ImmutableListMultimap.builder();
    private final ImmutableMultimap.Builder<CqlIdentifier, AdminRow> aggregatesBuilder =
        ImmutableListMultimap.builder();
    private final Map<CqlIdentifier, ImmutableMultimap.Builder<CqlIdentifier, AdminRow>>
        columnsBuilders = new LinkedHashMap<>();
    private final Map<CqlIdentifier, ImmutableMultimap.Builder<CqlIdentifier, AdminRow>>
        indexesBuilders = new LinkedHashMap<>();

    public Builder(
        boolean isCassandraV3, CompletableFuture<Metadata> refreshFuture, String logPrefix) {
      this.isCassandraV3 = isCassandraV3;
      this.refreshFuture = refreshFuture;
      this.logPrefix = logPrefix;
      this.tableNameColumn = isCassandraV3 ? "table_name" : "columnfamily_name";
    }

    public Builder withKeyspaces(Iterable<AdminRow> rows) {
      keyspacesBuilder.addAll(rows);
      return this;
    }

    public Builder withTables(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspace(row, tablesBuilder);
      }
      return this;
    }

    public Builder withViews(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspace(row, viewsBuilder);
      }
      return this;
    }

    public Builder withTypes(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspace(row, typesBuilder);
      }
      return this;
    }

    public Builder withFunctions(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspace(row, functionsBuilder);
      }
      return this;
    }

    public Builder withAggregates(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspace(row, aggregatesBuilder);
      }
      return this;
    }

    public Builder withColumns(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspaceAndTable(row, columnsBuilders);
      }
      return this;
    }

    public Builder withIndexes(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspaceAndTable(row, indexesBuilders);
      }
      return this;
    }

    private void putByKeyspace(
        AdminRow row, ImmutableMultimap.Builder<CqlIdentifier, AdminRow> builder) {
      String keyspace = row.getString("keyspace_name");
      if (keyspace == null) {
        LOG.warn("[{}] Skipping system row with missing keyspace name", logPrefix);
      } else {
        builder.put(CqlIdentifier.fromInternal(keyspace), row);
      }
    }

    private void putByKeyspaceAndTable(
        AdminRow row,
        Map<CqlIdentifier, ImmutableMultimap.Builder<CqlIdentifier, AdminRow>> builders) {
      String keyspace = row.getString("keyspace_name");
      String table = row.getString(tableNameColumn);
      if (keyspace == null) {
        LOG.warn("[{}] Skipping system row with missing keyspace name", logPrefix);
      } else if (table == null) {
        LOG.warn("[{}] Skipping system row with missing table name", logPrefix);
      } else {
        ImmutableMultimap.Builder<CqlIdentifier, AdminRow> builder =
            builders.computeIfAbsent(
                CqlIdentifier.fromInternal(keyspace), s -> ImmutableListMultimap.builder());
        builder.put(CqlIdentifier.fromInternal(table), row);
      }
    }

    public CassandraSchemaRows build() {
      return new CassandraSchemaRows(
          isCassandraV3,
          refreshFuture,
          keyspacesBuilder.build(),
          tablesBuilder.build(),
          viewsBuilder.build(),
          build(columnsBuilders),
          build(indexesBuilders),
          typesBuilder.build(),
          functionsBuilder.build(),
          aggregatesBuilder.build());
    }

    private static <K1, K2, V> Map<K1, Multimap<K2, V>> build(
        Map<K1, ImmutableMultimap.Builder<K2, V>> builders) {
      ImmutableMap.Builder<K1, Multimap<K2, V>> builder = ImmutableMap.builder();
      for (Map.Entry<K1, ImmutableMultimap.Builder<K2, V>> entry : builders.entrySet()) {
        builder.put(entry.getKey(), entry.getValue().build());
      }
      return builder.build();
    }
  }
}
