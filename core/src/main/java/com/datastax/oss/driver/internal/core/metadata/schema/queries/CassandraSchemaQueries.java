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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.util.Loggers;
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
  private final boolean isCassandraV3;
  private final String logPrefix;
  private final Duration timeout;
  private final int pageSize;
  private final String whereClause;
  // The future we return from execute, completes when all the queries are done.
  private final CompletableFuture<SchemaRows> schemaRowsFuture = new CompletableFuture<>();
  // A future that completes later, when the whole refresh is done. We just store it here to pass it
  // down to the next step.
  public final CompletableFuture<Metadata> refreshFuture;
  private final long startTimeNs = System.nanoTime();

  // All non-final fields are accessed exclusively on adminExecutor
  private CassandraSchemaRows.Builder schemaRowsBuilder;
  private int pendingQueries;

  protected CassandraSchemaQueries(
      DriverChannel channel,
      boolean isCassandraV3,
      CompletableFuture<Metadata> refreshFuture,
      DriverExecutionProfile config,
      String logPrefix) {
    this.channel = channel;
    this.adminExecutor = channel.eventLoop();
    this.isCassandraV3 = isCassandraV3;
    this.refreshFuture = refreshFuture;
    this.logPrefix = logPrefix;
    this.timeout = config.getDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT);
    this.pageSize = config.getInt(DefaultDriverOption.METADATA_SCHEMA_REQUEST_PAGE_SIZE);

    List<String> refreshedKeyspaces =
        config.getStringList(
            DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, Collections.emptyList());
    this.whereClause = buildWhereClause(refreshedKeyspaces);
  }

  private static String buildWhereClause(List<String> refreshedKeyspaces) {
    if (refreshedKeyspaces.isEmpty()) {
      return "";
    } else {
      StringBuilder builder = new StringBuilder(" WHERE keyspace_name in (");
      boolean first = true;
      for (String keyspace : refreshedKeyspaces) {
        if (first) {
          first = false;
        } else {
          builder.append(",");
        }
        builder.append('\'').append(keyspace).append('\'');
      }
      return builder.append(")").toString();
    }
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

  @Override
  public CompletionStage<SchemaRows> execute() {
    RunOrSchedule.on(adminExecutor, this::executeOnAdminExecutor);
    return schemaRowsFuture;
  }

  private void executeOnAdminExecutor() {
    assert adminExecutor.inEventLoop();

    schemaRowsBuilder = new CassandraSchemaRows.Builder(isCassandraV3, refreshFuture, logPrefix);

    query(selectKeyspacesQuery() + whereClause, schemaRowsBuilder::withKeyspaces, true);
    query(selectTypesQuery() + whereClause, schemaRowsBuilder::withTypes, true);
    query(selectTablesQuery() + whereClause, schemaRowsBuilder::withTables, true);
    query(selectColumnsQuery() + whereClause, schemaRowsBuilder::withColumns, true);
    selectIndexesQuery()
        .ifPresent(select -> query(select + whereClause, schemaRowsBuilder::withIndexes, true));
    selectViewsQuery()
        .ifPresent(select -> query(select + whereClause, schemaRowsBuilder::withViews, true));
    selectFunctionsQuery()
        .ifPresent(select -> query(select + whereClause, schemaRowsBuilder::withFunctions, true));
    selectAggregatesQuery()
        .ifPresent(select -> query(select + whereClause, schemaRowsBuilder::withAggregates, true));
    selectVirtualKeyspacesQuery()
        .ifPresent(
            select -> query(select + whereClause, schemaRowsBuilder::withVirtualKeyspaces, false));
    selectVirtualTablesQuery()
        .ifPresent(
            select -> query(select + whereClause, schemaRowsBuilder::withVirtualTables, false));
    selectVirtualColumnsQuery()
        .ifPresent(
            select -> query(select + whereClause, schemaRowsBuilder::withVirtualColumns, false));
  }

  private void query(
      String queryString,
      Function<Iterable<AdminRow>, CassandraSchemaRows.Builder> builderUpdater,
      boolean warnIfMissing) {
    assert adminExecutor.inEventLoop();

    pendingQueries += 1;
    query(queryString)
        .whenCompleteAsync(
            (result, error) -> handleResult(result, error, builderUpdater, warnIfMissing),
            adminExecutor);
  }

  @VisibleForTesting
  protected CompletionStage<AdminResult> query(String query) {
    return AdminRequestHandler.query(channel, query, timeout, pageSize, logPrefix).start();
  }

  /**
   * @param warnIfMissing whether to log a warning if the queried table does not exist: some DDAC
   *     versions report release_version > 4, but don't have a system_virtual_schema keyspace, so we
   *     want to ignore those errors silently.
   */
  private void handleResult(
      AdminResult result,
      Throwable error,
      Function<Iterable<AdminRow>, CassandraSchemaRows.Builder> builderUpdater,
      boolean warnIfMissing) {
    if (error != null) {
      if (warnIfMissing || !error.getMessage().contains("does not exist")) {
        Loggers.warnWithException(
            LOG,
            "[{}] Error during schema refresh, new metadata might be incomplete",
            logPrefix,
            error);
      }
      // Proceed without the results of this query, the rest of the schema refresh will run on a
      // "best effort" basis
      markQueryComplete();
    } else {
      // Store the rows of the current page in the builder
      schemaRowsBuilder = builderUpdater.apply(result);
      if (result.hasNextPage()) {
        result
            .nextPage()
            .whenCompleteAsync(
                (nextResult, nextError) ->
                    handleResult(nextResult, nextError, builderUpdater, warnIfMissing),
                adminExecutor);
      } else {
        markQueryComplete();
      }
    }
  }

  private void markQueryComplete() {
    pendingQueries -= 1;
    if (pendingQueries == 0) {
      LOG.debug("[{}] Schema queries took {}", logPrefix, NanoTime.formatTimeSince(startTimeNs));
      schemaRowsFuture.complete(schemaRowsBuilder.build());
    }
  }
}
