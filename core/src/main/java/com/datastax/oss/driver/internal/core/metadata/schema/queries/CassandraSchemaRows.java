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
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeClassNameParser;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeCqlNameParser;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeParser;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableListMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class CassandraSchemaRows implements SchemaRows {

  private final Node node;
  private final DataTypeParser dataTypeParser;
  private final List<AdminRow> keyspaces;
  private final List<AdminRow> virtualKeyspaces;
  private final Multimap<CqlIdentifier, AdminRow> tables;
  private final Multimap<CqlIdentifier, AdminRow> virtualTables;
  private final Multimap<CqlIdentifier, AdminRow> views;
  private final Multimap<CqlIdentifier, AdminRow> types;
  private final Multimap<CqlIdentifier, AdminRow> functions;
  private final Multimap<CqlIdentifier, AdminRow> aggregates;
  private final Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> columns;
  private final Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> virtualColumns;
  private final Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> indexes;
  private final Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> vertices;
  private final Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> edges;

  private CassandraSchemaRows(
      Node node,
      DataTypeParser dataTypeParser,
      List<AdminRow> keyspaces,
      List<AdminRow> virtualKeyspaces,
      Multimap<CqlIdentifier, AdminRow> tables,
      Multimap<CqlIdentifier, AdminRow> virtualTables,
      Multimap<CqlIdentifier, AdminRow> views,
      Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> columns,
      Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> virtualColumns,
      Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> indexes,
      Multimap<CqlIdentifier, AdminRow> types,
      Multimap<CqlIdentifier, AdminRow> functions,
      Multimap<CqlIdentifier, AdminRow> aggregates,
      Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> vertices,
      Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> edges) {
    this.node = node;
    this.dataTypeParser = dataTypeParser;
    this.keyspaces = keyspaces;
    this.virtualKeyspaces = virtualKeyspaces;
    this.tables = tables;
    this.virtualTables = virtualTables;
    this.views = views;
    this.columns = columns;
    this.virtualColumns = virtualColumns;
    this.indexes = indexes;
    this.types = types;
    this.functions = functions;
    this.aggregates = aggregates;
    this.vertices = vertices;
    this.edges = edges;
  }

  @NonNull
  @Override
  public Node getNode() {
    return node;
  }

  @Override
  public DataTypeParser dataTypeParser() {
    return dataTypeParser;
  }

  @Override
  public List<AdminRow> keyspaces() {
    return keyspaces;
  }

  @Override
  public List<AdminRow> virtualKeyspaces() {
    return virtualKeyspaces;
  }

  @Override
  public Multimap<CqlIdentifier, AdminRow> tables() {
    return tables;
  }

  @Override
  public Multimap<CqlIdentifier, AdminRow> virtualTables() {
    return virtualTables;
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
  public Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> virtualColumns() {
    return virtualColumns;
  }

  @Override
  public Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> indexes() {
    return indexes;
  }

  @Override
  public Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> vertices() {
    return vertices;
  }

  @Override
  public Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> edges() {
    return edges;
  }

  public static class Builder {
    private static final Logger LOG = LoggerFactory.getLogger(Builder.class);

    private final Node node;
    private final DataTypeParser dataTypeParser;
    private final String tableNameColumn;
    private final KeyspaceFilter keyspaceFilter;
    private final String logPrefix;
    private final ImmutableList.Builder<AdminRow> keyspacesBuilder = ImmutableList.builder();
    private final ImmutableList.Builder<AdminRow> virtualKeyspacesBuilder = ImmutableList.builder();
    private final ImmutableMultimap.Builder<CqlIdentifier, AdminRow> tablesBuilder =
        ImmutableListMultimap.builder();
    private final ImmutableMultimap.Builder<CqlIdentifier, AdminRow> virtualTablesBuilder =
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
        virtualColumnsBuilders = new LinkedHashMap<>();
    private final Map<CqlIdentifier, ImmutableMultimap.Builder<CqlIdentifier, AdminRow>>
        indexesBuilders = new LinkedHashMap<>();
    private final Map<CqlIdentifier, ImmutableMultimap.Builder<CqlIdentifier, AdminRow>>
        verticesBuilders = new LinkedHashMap<>();
    private final Map<CqlIdentifier, ImmutableMultimap.Builder<CqlIdentifier, AdminRow>>
        edgesBuilders = new LinkedHashMap<>();

    public Builder(Node node, KeyspaceFilter keyspaceFilter, String logPrefix) {
      this.node = node;
      this.keyspaceFilter = keyspaceFilter;
      this.logPrefix = logPrefix;
      if (isCassandraV3OrAbove(node)) {
        this.tableNameColumn = "table_name";
        this.dataTypeParser = new DataTypeCqlNameParser();
      } else {
        this.tableNameColumn = "columnfamily_name";
        this.dataTypeParser = new DataTypeClassNameParser();
      }
    }

    private static boolean isCassandraV3OrAbove(Node node) {
      // We already did those checks in DefaultSchemaQueriesFactory.
      // We could pass along booleans (isCassandraV3, isDse...), but passing the whole Node is
      // better for maintainability, in case we need to do more checks in downstream components in
      // the future.
      Version dseVersion = (Version) node.getExtras().get(DseNodeProperties.DSE_VERSION);
      if (dseVersion != null) {
        dseVersion = dseVersion.nextStable();
        return dseVersion.compareTo(Version.V5_0_0) >= 0;
      } else {
        Version cassandraVersion = node.getCassandraVersion();
        if (cassandraVersion == null) {
          cassandraVersion = Version.V3_0_0;
        } else {
          cassandraVersion = cassandraVersion.nextStable();
        }
        return cassandraVersion.compareTo(Version.V3_0_0) >= 0;
      }
    }

    public Builder withKeyspaces(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        put(keyspacesBuilder, row);
      }
      return this;
    }

    public Builder withVirtualKeyspaces(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        put(virtualKeyspacesBuilder, row);
      }
      return this;
    }

    public Builder withTables(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspace(row, tablesBuilder);
      }
      return this;
    }

    public Builder withVirtualTables(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspace(row, virtualTablesBuilder);
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

    public Builder withVirtualColumns(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspaceAndTable(row, virtualColumnsBuilders);
      }
      return this;
    }

    public Builder withIndexes(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspaceAndTable(row, indexesBuilders);
      }
      return this;
    }

    public Builder withVertices(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspaceAndTable(row, verticesBuilders);
      }
      return this;
    }

    public Builder withEdges(Iterable<AdminRow> rows) {
      for (AdminRow row : rows) {
        putByKeyspaceAndTable(row, edgesBuilders);
      }
      return this;
    }

    private void put(ImmutableList.Builder<AdminRow> builder, AdminRow row) {
      String keyspace = row.getString("keyspace_name");
      if (keyspace == null) {
        LOG.warn("[{}] Skipping system row with missing keyspace name", logPrefix);
      } else if (keyspaceFilter.includes(keyspace)) {
        builder.add(row);
      }
    }

    private void putByKeyspace(
        AdminRow row, ImmutableMultimap.Builder<CqlIdentifier, AdminRow> builder) {
      String keyspace = row.getString("keyspace_name");
      if (keyspace == null) {
        LOG.warn("[{}] Skipping system row with missing keyspace name", logPrefix);
      } else if (keyspaceFilter.includes(keyspace)) {
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
      } else if (keyspaceFilter.includes(keyspace)) {
        ImmutableMultimap.Builder<CqlIdentifier, AdminRow> builder =
            builders.computeIfAbsent(
                CqlIdentifier.fromInternal(keyspace), s -> ImmutableListMultimap.builder());
        builder.put(CqlIdentifier.fromInternal(table), row);
      }
    }

    public CassandraSchemaRows build() {
      return new CassandraSchemaRows(
          node,
          dataTypeParser,
          keyspacesBuilder.build(),
          virtualKeyspacesBuilder.build(),
          tablesBuilder.build(),
          virtualTablesBuilder.build(),
          viewsBuilder.build(),
          build(columnsBuilders),
          build(virtualColumnsBuilders),
          build(indexesBuilders),
          typesBuilder.build(),
          functionsBuilder.build(),
          aggregatesBuilder.build(),
          build(verticesBuilders),
          build(edgesBuilders));
    }

    private static <K1, K2, V> Map<K1, Multimap<K2, V>> build(
        Map<K1, ImmutableMultimap.Builder<K2, V>> builders) {
      ImmutableMap.Builder<K1, Multimap<K2, V>> builder = ImmutableMap.builder();
      builders
          .entrySet()
          .forEach(
              (entry) -> {
                builder.put(entry.getKey(), entry.getValue().build());
              });
      return builder.build();
    }
  }
}
