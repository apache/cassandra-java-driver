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
package com.datastax.dse.driver.internal.core.metadata.schema.queries;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeCqlNameParser;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeParser;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableListMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dse68SchemaRows implements SchemaRows {

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
  private final Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> vertices;
  private final Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> edges;
  private final List<AdminRow> virtualKeyspaces;
  private final Multimap<CqlIdentifier, AdminRow> virtualTables;
  private final Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> virtualColumns;

  private Dse68SchemaRows(
      CompletableFuture<Metadata> refreshFuture,
      List<AdminRow> keyspaces,
      Multimap<CqlIdentifier, AdminRow> tables,
      Multimap<CqlIdentifier, AdminRow> views,
      Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> columns,
      Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> indexes,
      Multimap<CqlIdentifier, AdminRow> types,
      Multimap<CqlIdentifier, AdminRow> functions,
      Multimap<CqlIdentifier, AdminRow> aggregates,
      List<AdminRow> virtualKeyspaces,
      Multimap<CqlIdentifier, AdminRow> virtualTables,
      Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> virtualColumns,
      Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> vertices,
      Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> edges) {
    this.dataTypeParser = new DataTypeCqlNameParser();
    this.refreshFuture = refreshFuture;
    this.keyspaces = keyspaces;
    this.tables = tables;
    this.views = views;
    this.columns = columns;
    this.indexes = indexes;
    this.types = types;
    this.functions = functions;
    this.aggregates = aggregates;
    this.virtualKeyspaces = virtualKeyspaces;
    this.virtualTables = virtualTables;
    this.virtualColumns = virtualColumns;
    this.vertices = vertices;
    this.edges = edges;
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

  @Override
  public List<AdminRow> virtualKeyspaces() {
    return virtualKeyspaces;
  }

  @Override
  public Multimap<CqlIdentifier, AdminRow> virtualTables() {
    return virtualTables;
  }

  @Override
  public Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> virtualColumns() {
    return virtualColumns;
  }

  public Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> vertices() {
    return vertices;
  }

  public Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> edges() {
    return edges;
  }

  public static class Builder {
    private static final Logger LOG = LoggerFactory.getLogger(Dse68SchemaRows.Builder.class);

    private final CompletableFuture<Metadata> refreshFuture;
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
    private final Map<CqlIdentifier, ImmutableMultimap.Builder<CqlIdentifier, AdminRow>>
        verticesBuilders = new LinkedHashMap<>();
    private final Map<CqlIdentifier, ImmutableMultimap.Builder<CqlIdentifier, AdminRow>>
        edgesBuilders = new LinkedHashMap<>();
    private final ImmutableList.Builder<AdminRow> virtualKeyspacesBuilder = ImmutableList.builder();
    private final ImmutableMultimap.Builder<CqlIdentifier, AdminRow> virtualTablesBuilder =
        ImmutableListMultimap.builder();
    private final Map<CqlIdentifier, ImmutableMultimap.Builder<CqlIdentifier, AdminRow>>
        virtualColumnsBuilders = new LinkedHashMap<>();

    public Builder(CompletableFuture<Metadata> refreshFuture, String logPrefix) {
      this.refreshFuture = refreshFuture;
      this.logPrefix = logPrefix;
    }

    public Dse68SchemaRows.Builder withKeyspaces(Iterable<AdminRow> rows) {
      keyspacesBuilder.addAll(rows);
      return this;
    }

    public Dse68SchemaRows.Builder withTables(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspace(row, tablesBuilder);
      }
      return this;
    }

    public Dse68SchemaRows.Builder withViews(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspace(row, viewsBuilder);
      }
      return this;
    }

    public Dse68SchemaRows.Builder withTypes(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspace(row, typesBuilder);
      }
      return this;
    }

    public Dse68SchemaRows.Builder withFunctions(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspace(row, functionsBuilder);
      }
      return this;
    }

    public Dse68SchemaRows.Builder withAggregates(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspace(row, aggregatesBuilder);
      }
      return this;
    }

    public Dse68SchemaRows.Builder withColumns(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspaceAndTable(row, columnsBuilders);
      }
      return this;
    }

    public Dse68SchemaRows.Builder withIndexes(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspaceAndTable(row, indexesBuilders);
      }
      return this;
    }

    public Dse68SchemaRows.Builder withVirtualKeyspaces(Iterable<AdminRow> rows) {
      virtualKeyspacesBuilder.addAll(rows);
      return this;
    }

    public Dse68SchemaRows.Builder withVirtualTables(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspace(row, virtualTablesBuilder);
      }
      return this;
    }

    public Dse68SchemaRows.Builder withVirtualColumns(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspaceAndTable(row, virtualColumnsBuilders);
      }
      return this;
    }

    public Dse68SchemaRows.Builder withVertices(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspaceAndTable(row, verticesBuilders);
      }
      return this;
    }

    public Dse68SchemaRows.Builder withEdges(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspaceAndTable(row, edgesBuilders);
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
      String table = row.getString("table_name");
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

    public Dse68SchemaRows build() {
      return new Dse68SchemaRows(
          refreshFuture,
          keyspacesBuilder.build(),
          tablesBuilder.build(),
          viewsBuilder.build(),
          build(columnsBuilders),
          build(indexesBuilders),
          typesBuilder.build(),
          functionsBuilder.build(),
          aggregatesBuilder.build(),
          virtualKeyspacesBuilder.build(),
          virtualTablesBuilder.build(),
          build(virtualColumnsBuilders),
          build(verticesBuilders),
          build(edgesBuilders));
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
