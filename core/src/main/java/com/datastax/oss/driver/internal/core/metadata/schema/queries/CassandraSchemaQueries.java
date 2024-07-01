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
/*
 * Copyright (C) 2024 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.util.NanoTime;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.EventExecutor;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public abstract class CassandraSchemaQueries implements SchemaQueries {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraSchemaQueries.class);

  private final DriverChannel channel;
  private final EventExecutor adminExecutor;
  private final Node node;
  private final String logPrefix;
  private final Duration timeout;
  private final int pageSize;
  private final KeyspaceFilter keyspaceFilter;
  // The future we return from execute, completes when all the queries are done.
  private final CompletableFuture<SchemaRows> schemaRowsFuture = new CompletableFuture<>();
  private final long startTimeNs = System.nanoTime();
  private final String usingTimeoutClause;

  // All non-final fields are accessed exclusively on adminExecutor
  private CassandraSchemaRows.Builder schemaRowsBuilder;
  private int pendingQueries;

  protected CassandraSchemaQueries(
      DriverChannel channel, Node node, DriverExecutionProfile config, String logPrefix) {
    this.channel = channel;
    this.adminExecutor = channel.eventLoop();
    this.node = node;
    this.logPrefix = logPrefix;
    this.timeout = config.getDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT);
    this.pageSize = config.getInt(DefaultDriverOption.METADATA_SCHEMA_REQUEST_PAGE_SIZE);

    List<String> refreshedKeyspaces =
        config.getStringList(
            DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, Collections.emptyList());
    assert refreshedKeyspaces != null; // per the default value
    this.keyspaceFilter = KeyspaceFilter.newInstance(logPrefix, refreshedKeyspaces);
    this.usingTimeoutClause =
        " USING TIMEOUT "
            + config.getDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT).toMillis()
            + "ms";
  }

  protected abstract String selectKeyspacesQuery();

  protected abstract Optional<String> selectVirtualKeyspacesQuery();

  protected abstract String selectTablesQuery();

  protected abstract Optional<String> selectVirtualTablesQuery();

  protected abstract Optional<String> selectViewsQuery();

  protected abstract Optional<String> selectIndexesQuery();

  protected abstract String selectColumnsQuery();

  protected abstract Optional<String> selectVirtualColumnsQuery();

  protected abstract String selectTypesQuery();

  protected abstract Optional<String> selectFunctionsQuery();

  protected abstract Optional<String> selectAggregatesQuery();

  protected abstract Optional<String> selectEdgesQuery();

  protected abstract Optional<String> selectVerticiesQuery();

  @Override
  public CompletionStage<SchemaRows> execute() {
    RunOrSchedule.on(adminExecutor, this::executeOnAdminExecutor);
    return schemaRowsFuture;
  }

  private void executeOnAdminExecutor() {
    assert adminExecutor.inEventLoop();

    schemaRowsBuilder = new CassandraSchemaRows.Builder(node, keyspaceFilter, logPrefix);
    String whereClause = keyspaceFilter.getWhereClause();
    String usingClause = shouldApplyUsingTimeout() ? usingTimeoutClause : "";

    query(selectKeyspacesQuery() + whereClause + usingClause, schemaRowsBuilder::withKeyspaces);
    query(selectTypesQuery() + whereClause + usingClause, schemaRowsBuilder::withTypes);
    query(selectTablesQuery() + whereClause + usingClause, schemaRowsBuilder::withTables);
    query(selectColumnsQuery() + whereClause + usingClause, schemaRowsBuilder::withColumns);
    selectIndexesQuery()
        .ifPresent(
            select -> query(select + whereClause + usingClause, schemaRowsBuilder::withIndexes));
    selectViewsQuery()
        .ifPresent(
            select -> query(select + whereClause + usingClause, schemaRowsBuilder::withViews));
    selectFunctionsQuery()
        .ifPresent(
            select -> query(select + whereClause + usingClause, schemaRowsBuilder::withFunctions));
    selectAggregatesQuery()
        .ifPresent(
            select -> query(select + whereClause + usingClause, schemaRowsBuilder::withAggregates));
    selectVirtualKeyspacesQuery()
        .ifPresent(
            select ->
                query(select + whereClause + usingClause, schemaRowsBuilder::withVirtualKeyspaces));
    selectVirtualTablesQuery()
        .ifPresent(
            select ->
                query(select + whereClause + usingClause, schemaRowsBuilder::withVirtualTables));
    selectVirtualColumnsQuery()
        .ifPresent(
            select ->
                query(select + whereClause + usingClause, schemaRowsBuilder::withVirtualColumns));
    selectEdgesQuery()
        .ifPresent(
            select -> query(select + whereClause + usingClause, schemaRowsBuilder::withEdges));
    selectVerticiesQuery()
        .ifPresent(
            select -> query(select + whereClause + usingClause, schemaRowsBuilder::withVertices));
  }

  protected boolean shouldApplyUsingTimeout() {
    // We use non-null sharding info as a proxy check for cluster being a ScyllaDB cluster
    return (channel.getShardingInfo() != null);
  }

  private void query(
      String queryString,
      Function<Iterable<AdminRow>, CassandraSchemaRows.Builder> builderUpdater) {
    assert adminExecutor.inEventLoop();

    pendingQueries += 1;
    query(queryString)
        .whenCompleteAsync(
            (result, error) -> handleResult(result, error, builderUpdater), adminExecutor);
  }

  @VisibleForTesting
  protected CompletionStage<AdminResult> query(String query) {
    return AdminRequestHandler.query(channel, query, timeout, pageSize, logPrefix).start();
  }

  private void handleResult(
      AdminResult result,
      Throwable error,
      Function<Iterable<AdminRow>, CassandraSchemaRows.Builder> builderUpdater) {

    // If another query already failed, we've already propagated the failure so just ignore this one
    if (schemaRowsFuture.isCompletedExceptionally()) {
      return;
    }

    if (error != null) {
      schemaRowsFuture.completeExceptionally(error);
    } else {
      // Store the rows of the current page in the builder
      schemaRowsBuilder = builderUpdater.apply(result);
      if (result.hasNextPage()) {
        result
            .nextPage()
            .whenCompleteAsync(
                (nextResult, nextError) -> handleResult(nextResult, nextError, builderUpdater),
                adminExecutor);
      } else {
        pendingQueries -= 1;
        if (pendingQueries == 0) {
          LOG.debug(
              "[{}] Schema queries took {}", logPrefix, NanoTime.formatTimeSince(startTimeNs));
          schemaRowsFuture.complete(schemaRowsBuilder.build());
        }
      }
    }
  }
}
