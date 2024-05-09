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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.SchemaRefresh;
import com.datastax.oss.driver.internal.core.util.NanoTime;
import com.datastax.oss.driver.shaded.guava.common.base.MoreObjects;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default parser implementation for Cassandra.
 *
 * <p>For modularity, the code for each element row is split into separate classes (schema stuff is
 * not on the hot path, so creating a few extra objects doesn't matter).
 */
@ThreadSafe
public class CassandraSchemaParser implements SchemaParser {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraSchemaParser.class);

  private final SchemaRows rows;
  private final UserDefinedTypeParser userDefinedTypeParser;
  private final TableParser tableParser;
  private final ViewParser viewParser;
  private final FunctionParser functionParser;
  private final AggregateParser aggregateParser;
  private final String logPrefix;
  private final long startTimeNs = System.nanoTime();

  public CassandraSchemaParser(SchemaRows rows, InternalDriverContext context) {
    this.rows = rows;
    this.logPrefix = context.getSessionName();

    this.userDefinedTypeParser = new UserDefinedTypeParser(rows.dataTypeParser(), context);
    this.tableParser = new TableParser(rows, context);
    this.viewParser = new ViewParser(rows, context);
    this.functionParser = new FunctionParser(rows.dataTypeParser(), context);
    this.aggregateParser = new AggregateParser(rows.dataTypeParser(), context);
  }

  @Override
  public SchemaRefresh parse() {
    ImmutableMap.Builder<CqlIdentifier, KeyspaceMetadata> keyspacesBuilder = ImmutableMap.builder();
    for (AdminRow row : rows.keyspaces()) {
      KeyspaceMetadata keyspace = parseKeyspace(row);
      keyspacesBuilder.put(keyspace.getName(), keyspace);
    }
    for (AdminRow row : rows.virtualKeyspaces()) {
      KeyspaceMetadata keyspace = parseVirtualKeyspace(row);
      keyspacesBuilder.put(keyspace.getName(), keyspace);
    }
    SchemaRefresh refresh = new SchemaRefresh(keyspacesBuilder.build());
    LOG.debug("[{}] Schema parsing took {}", logPrefix, NanoTime.formatTimeSince(startTimeNs));
    return refresh;
  }

  private KeyspaceMetadata parseKeyspace(AdminRow keyspaceRow) {

    // Cassandra <= 2.2
    // CREATE TABLE system.schema_keyspaces (
    //     keyspace_name text PRIMARY KEY,
    //     durable_writes boolean,
    //     strategy_class text,
    //     strategy_options text
    // )
    //
    // Cassandra >= 3.0:
    // CREATE TABLE system_schema.keyspaces (
    //     keyspace_name text PRIMARY KEY,
    //     durable_writes boolean,
    //     replication frozen<map<text, text>>
    // )
    CqlIdentifier keyspaceId = CqlIdentifier.fromInternal(keyspaceRow.getString("keyspace_name"));
    boolean durableWrites =
        MoreObjects.firstNonNull(keyspaceRow.getBoolean("durable_writes"), false);

    Map<String, String> replicationOptions;
    if (keyspaceRow.contains("strategy_class")) {
      String strategyClass = keyspaceRow.getString("strategy_class");
      Map<String, String> strategyOptions =
          SimpleJsonParser.parseStringMap(keyspaceRow.getString("strategy_options"));
      replicationOptions =
          ImmutableMap.<String, String>builder()
              .putAll(strategyOptions)
              .put("class", strategyClass)
              .build();
    } else {
      replicationOptions = keyspaceRow.getMapOfStringToString("replication");
    }

    Map<CqlIdentifier, UserDefinedType> types = parseTypes(keyspaceId);

    return new DefaultKeyspaceMetadata(
        keyspaceId,
        durableWrites,
        false,
        replicationOptions,
        types,
        parseTables(keyspaceId, types),
        parseViews(keyspaceId, types),
        parseFunctions(keyspaceId, types),
        parseAggregates(keyspaceId, types));
  }

  private KeyspaceMetadata parseVirtualKeyspace(AdminRow keyspaceRow) {

    CqlIdentifier keyspaceId = CqlIdentifier.fromInternal(keyspaceRow.getString("keyspace_name"));
    boolean durableWrites =
        MoreObjects.firstNonNull(keyspaceRow.getBoolean("durable_writes"), false);

    Map<String, String> replicationOptions = Collections.emptyMap();
    ;

    Map<CqlIdentifier, UserDefinedType> types = parseTypes(keyspaceId);

    return new DefaultKeyspaceMetadata(
        keyspaceId,
        durableWrites,
        true,
        replicationOptions,
        types,
        parseVirtualTables(keyspaceId, types),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap());
  }

  private Map<CqlIdentifier, UserDefinedType> parseTypes(CqlIdentifier keyspaceId) {
    return userDefinedTypeParser.parse(rows.types().get(keyspaceId), keyspaceId);
  }

  private Map<CqlIdentifier, TableMetadata> parseVirtualTables(
      CqlIdentifier keyspaceId, Map<CqlIdentifier, UserDefinedType> types) {
    ImmutableMap.Builder<CqlIdentifier, TableMetadata> tablesBuilder = ImmutableMap.builder();
    for (AdminRow tableRow : rows.virtualTables().get(keyspaceId)) {
      TableMetadata table = tableParser.parseVirtualTable(tableRow, keyspaceId, types);
      if (table != null) {
        tablesBuilder.put(table.getName(), table);
      }
    }
    return tablesBuilder.build();
  }

  private Map<CqlIdentifier, TableMetadata> parseTables(
      CqlIdentifier keyspaceId, Map<CqlIdentifier, UserDefinedType> types) {
    ImmutableMap.Builder<CqlIdentifier, TableMetadata> tablesBuilder = ImmutableMap.builder();
    for (AdminRow tableRow : rows.tables().get(keyspaceId)) {
      TableMetadata table = tableParser.parseTable(tableRow, keyspaceId, types);
      if (table != null) {
        tablesBuilder.put(table.getName(), table);
      }
    }
    return tablesBuilder.build();
  }

  private Map<CqlIdentifier, ViewMetadata> parseViews(
      CqlIdentifier keyspaceId, Map<CqlIdentifier, UserDefinedType> types) {
    ImmutableMap.Builder<CqlIdentifier, ViewMetadata> viewsBuilder = ImmutableMap.builder();
    for (AdminRow viewRow : rows.views().get(keyspaceId)) {
      ViewMetadata view = viewParser.parseView(viewRow, keyspaceId, types);
      if (view != null) {
        viewsBuilder.put(view.getName(), view);
      }
    }
    return viewsBuilder.build();
  }

  private Map<FunctionSignature, FunctionMetadata> parseFunctions(
      CqlIdentifier keyspaceId, Map<CqlIdentifier, UserDefinedType> types) {
    ImmutableMap.Builder<FunctionSignature, FunctionMetadata> functionsBuilder =
        ImmutableMap.builder();
    for (AdminRow functionRow : rows.functions().get(keyspaceId)) {
      FunctionMetadata function = functionParser.parseFunction(functionRow, keyspaceId, types);
      if (function != null) {
        functionsBuilder.put(function.getSignature(), function);
      }
    }
    return functionsBuilder.build();
  }

  private Map<FunctionSignature, AggregateMetadata> parseAggregates(
      CqlIdentifier keyspaceId, Map<CqlIdentifier, UserDefinedType> types) {
    ImmutableMap.Builder<FunctionSignature, AggregateMetadata> aggregatesBuilder =
        ImmutableMap.builder();
    for (AdminRow aggregateRow : rows.aggregates().get(keyspaceId)) {
      AggregateMetadata aggregate = aggregateParser.parseAggregate(aggregateRow, keyspaceId, types);
      if (aggregate != null) {
        aggregatesBuilder.put(aggregate.getSignature(), aggregate);
      }
    }
    return aggregatesBuilder.build();
  }
}
