/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.metadata.schema.queries;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaQueries;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.NanoTime;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.EventExecutor;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The system table queries to refresh the schema in DSE 6.8.
 *
 * <p>There are two additional tables for per-table graph metadata.
 */
public class Dse68SchemaQueries implements SchemaQueries {

  private static final Logger LOG = LoggerFactory.getLogger(Dse68SchemaQueries.class);

  private final DriverChannel channel;
  private final EventExecutor adminExecutor;
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
  private Dse68SchemaRows.Builder schemaRowsBuilder;
  private int pendingQueries;

  public Dse68SchemaQueries(
      DriverChannel channel,
      CompletableFuture<Metadata> refreshFuture,
      DriverExecutionProfile config,
      String logPrefix) {
    this.channel = channel;
    this.adminExecutor = channel.eventLoop();
    this.refreshFuture = refreshFuture;
    this.logPrefix = logPrefix;
    this.timeout = config.getDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT);
    this.pageSize = config.getInt(DefaultDriverOption.METADATA_SCHEMA_REQUEST_PAGE_SIZE);

    List<String> refreshedKeyspaces =
        config.isDefined(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES)
            ? config.getStringList(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES)
            : Collections.emptyList();
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

  @Override
  public CompletionStage<SchemaRows> execute() {
    RunOrSchedule.on(adminExecutor, this::executeOnAdminExecutor);
    return schemaRowsFuture;
  }

  private void executeOnAdminExecutor() {
    assert adminExecutor.inEventLoop();

    schemaRowsBuilder = new Dse68SchemaRows.Builder(refreshFuture, logPrefix);

    query(
        "SELECT * FROM system_schema.keyspaces" + whereClause,
        schemaRowsBuilder::withKeyspaces,
        true);
    query("SELECT * FROM system_schema.types" + whereClause, schemaRowsBuilder::withTypes, true);
    query("SELECT * FROM system_schema.tables" + whereClause, schemaRowsBuilder::withTables, true);
    query(
        "SELECT * FROM system_schema.columns" + whereClause, schemaRowsBuilder::withColumns, true);
    query(
        "SELECT * FROM system_schema.indexes" + whereClause, schemaRowsBuilder::withIndexes, true);
    query("SELECT * FROM system_schema.views" + whereClause, schemaRowsBuilder::withViews, true);
    query(
        "SELECT * FROM system_schema.functions" + whereClause,
        schemaRowsBuilder::withFunctions,
        true);
    query(
        "SELECT * FROM system_schema.aggregates" + whereClause,
        schemaRowsBuilder::withAggregates,
        true);
    // Virtual tables (DSE 6.7+, C* 4.0+)
    query(
        "SELECT * FROM system_virtual_schema.keyspaces" + whereClause,
        schemaRowsBuilder::withVirtualKeyspaces,
        false);
    query(
        "SELECT * FROM system_virtual_schema.tables" + whereClause,
        schemaRowsBuilder::withVirtualTables,
        false);
    query(
        "SELECT * FROM system_virtual_schema.columns" + whereClause,
        schemaRowsBuilder::withVirtualColumns,
        false);
    // Graph metadata (DSE 6.8+)
    query(
        "SELECT * FROM system_schema.vertices" + whereClause,
        schemaRowsBuilder::withVertices,
        true);
    query("SELECT * FROM system_schema.edges" + whereClause, schemaRowsBuilder::withEdges, true);
  }

  private void query(
      String queryString,
      Function<Iterable<AdminRow>, Dse68SchemaRows.Builder> builderUpdater,
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

  private void handleResult(
      AdminResult result,
      Throwable error,
      Function<Iterable<AdminRow>, Dse68SchemaRows.Builder> builderUpdater,
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
