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

import java.util.concurrent.CompletionStage;

/**
 * Manages the queries to system tables during a schema refresh.
 *
 * <p>They are all asynchronous, and possibly paged. This class abstracts all the details and
 * exposes a common result type.
 *
 * <p>Implementations must be thread-safe.
 */
<<<<<<< HEAD
public interface SchemaQueries {
=======
@ThreadSafe
public abstract class SchemaQueries {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaQueries.class);

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
  private SchemaRows.Builder schemaRowsBuilder;
  private int pendingQueries;

  protected SchemaQueries(
      DriverChannel channel,
      boolean isCassandraV3,
      CompletableFuture<Metadata> refreshFuture,
      DriverConfigProfile config,
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

  protected abstract String selectTablesQuery();

  protected abstract Optional<String> selectViewsQuery();

  protected abstract Optional<String> selectIndexesQuery();

  protected abstract String selectColumnsQuery();

  protected abstract String selectTypesQuery();

  protected abstract Optional<String> selectFunctionsQuery();

  protected abstract Optional<String> selectAggregatesQuery();

  protected abstract Optional<String> selectVirtualKeyspaces();

  protected abstract Optional<String> selectVirtualTables();

  protected abstract Optional<String> selectVirtualColumns();

  public CompletionStage<SchemaRows> execute() {
    RunOrSchedule.on(adminExecutor, this::executeOnAdminExecutor);
    return schemaRowsFuture;
  }

  private void executeOnAdminExecutor() {
    assert adminExecutor.inEventLoop();

    schemaRowsBuilder = new SchemaRows.Builder(isCassandraV3, refreshFuture, logPrefix);

    query(selectKeyspacesQuery() + whereClause, schemaRowsBuilder::withKeyspaces);
    query(selectTypesQuery() + whereClause, schemaRowsBuilder::withTypes);
    query(selectTablesQuery() + whereClause, schemaRowsBuilder::withTables);
    query(selectColumnsQuery() + whereClause, schemaRowsBuilder::withColumns);
    selectIndexesQuery()
        .ifPresent(select -> query(select + whereClause, schemaRowsBuilder::withIndexes));
    selectViewsQuery()
        .ifPresent(select -> query(select + whereClause, schemaRowsBuilder::withViews));
    selectFunctionsQuery()
        .ifPresent(select -> query(select + whereClause, schemaRowsBuilder::withFunctions));
    selectAggregatesQuery()
        .ifPresent(select -> query(select + whereClause, schemaRowsBuilder::withAggregates));
    selectVirtualKeyspaces()
        .ifPresent(select -> query(select + whereClause, schemaRowsBuilder::withVirtualKeyspaces));
    selectVirtualTables()
        .ifPresent(select -> query(select + whereClause, schemaRowsBuilder::withVirtualTables));
    selectVirtualColumns()
        .ifPresent(select -> query(select + whereClause, schemaRowsBuilder::withVirtualColumns));
  }

  private void query(
      String queryString, Function<Iterable<AdminRow>, SchemaRows.Builder> builderUpdater) {
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
>>>>>>> Adding system views

  /**
   * Launch the queries asynchronously, returning a future that will complete when they have all
   * succeeded.
   */
  CompletionStage<SchemaRows> execute();
}
