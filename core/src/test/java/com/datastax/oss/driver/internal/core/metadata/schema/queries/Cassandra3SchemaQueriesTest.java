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

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.time.Duration;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingDeque;
import org.junit.Before;
import org.junit.Test;

public class Cassandra3SchemaQueriesTest extends SchemaQueriesTest {

  @Before
  @Override
  public void setup() {
    super.setup();

    // By default, no keyspace filter
    when(config.getStringList(
            DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, Collections.emptyList()))
        .thenReturn(Collections.emptyList());
    when(node.getCassandraVersion()).thenReturn(Version.V3_0_0);
  }

  @Test
  public void should_query_without_keyspace_filter() {
    should_query_with_where_clause("");
  }

  @Test
  public void should_query_with_keyspace_filter() {
    when(config.getStringList(
            DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, Collections.emptyList()))
        .thenReturn(ImmutableList.of("ks1", "ks2"));

    should_query_with_where_clause(" WHERE keyspace_name IN ('ks1','ks2')");
  }

  @Test
  public void should_query_with_using_clause() {
    when(config.getDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT))
        .thenReturn(Duration.ofMillis(100));
    should_query_with_clauses("", " USING TIMEOUT 100ms");
  }

  private void should_query_with_where_clause(String whereClause) {
    should_query_with_clauses(whereClause, "");
  }

  private void should_query_with_clauses(String whereClause, String usingClause) {
    SchemaQueriesWithMockedChannel queries =
        new SchemaQueriesWithMockedChannel(
            driverChannel, node, config, "test", !usingClause.equals(""));
    CompletionStage<SchemaRows> result = queries.execute();

    // Keyspace
    Call call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.keyspaces" + whereClause + usingClause);
    call.result.complete(
        mockResult(mockRow("keyspace_name", "ks1"), mockRow("keyspace_name", "ks2")));

    // Types
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.types" + whereClause + usingClause);
    call.result.complete(mockResult(mockRow("keyspace_name", "ks1", "type_name", "type")));

    // Tables
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.tables" + whereClause + usingClause);
    call.result.complete(mockResult(mockRow("keyspace_name", "ks1", "table_name", "foo")));

    // Columns
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.columns" + whereClause + usingClause);
    call.result.complete(
        mockResult(mockRow("keyspace_name", "ks1", "table_name", "foo", "column_name", "k")));

    // Indexes
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.indexes" + whereClause + usingClause);
    call.result.complete(
        mockResult(mockRow("keyspace_name", "ks1", "table_name", "foo", "index_name", "index")));

    // Views
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.views" + whereClause + usingClause);
    call.result.complete(mockResult(mockRow("keyspace_name", "ks2", "view_name", "foo")));

    // Functions
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.functions" + whereClause + usingClause);
    call.result.complete(mockResult(mockRow("keyspace_name", "ks2", "function_name", "add")));

    // Aggregates
    call = queries.calls.poll();
    assertThat(call.query)
        .isEqualTo("SELECT * FROM system_schema.aggregates" + whereClause + usingClause);
    call.result.complete(mockResult(mockRow("keyspace_name", "ks2", "aggregate_name", "add")));

    channel.runPendingTasks();

    assertThatStage(result)
        .isSuccess(
            rows -> {
              assertThat(rows.getNode()).isEqualTo(node);

              // Keyspace
              assertThat(rows.keyspaces()).hasSize(2);
              assertThat(rows.keyspaces().get(0).getString("keyspace_name")).isEqualTo("ks1");
              assertThat(rows.keyspaces().get(1).getString("keyspace_name")).isEqualTo("ks2");

              // Types
              assertThat(rows.types().keySet()).containsOnly(KS1_ID);
              assertThat(rows.types().get(KS1_ID)).hasSize(1);
              assertThat(rows.types().get(KS1_ID).iterator().next().getString("type_name"))
                  .isEqualTo("type");

              // Tables
              assertThat(rows.tables().keySet()).containsOnly(KS1_ID);
              assertThat(rows.tables().get(KS1_ID)).hasSize(1);
              assertThat(rows.tables().get(KS1_ID).iterator().next().getString("table_name"))
                  .isEqualTo("foo");

              // Columns
              assertThat(rows.columns().keySet()).containsOnly(KS1_ID);
              assertThat(rows.columns().get(KS1_ID).keySet()).containsOnly(FOO_ID);
              assertThat(
                      rows.columns()
                          .get(KS1_ID)
                          .get(FOO_ID)
                          .iterator()
                          .next()
                          .getString("column_name"))
                  .isEqualTo("k");

              // Indexes
              assertThat(rows.indexes().keySet()).containsOnly(KS1_ID);
              assertThat(rows.indexes().get(KS1_ID).keySet()).containsOnly(FOO_ID);
              assertThat(
                      rows.indexes()
                          .get(KS1_ID)
                          .get(FOO_ID)
                          .iterator()
                          .next()
                          .getString("index_name"))
                  .isEqualTo("index");

              // Views
              assertThat(rows.views().keySet()).containsOnly(KS2_ID);
              assertThat(rows.views().get(KS2_ID)).hasSize(1);
              assertThat(rows.views().get(KS2_ID).iterator().next().getString("view_name"))
                  .isEqualTo("foo");

              // Functions
              assertThat(rows.functions().keySet()).containsOnly(KS2_ID);
              assertThat(rows.functions().get(KS2_ID)).hasSize(1);
              assertThat(rows.functions().get(KS2_ID).iterator().next().getString("function_name"))
                  .isEqualTo("add");

              // Aggregates
              assertThat(rows.aggregates().keySet()).containsOnly(KS2_ID);
              assertThat(rows.aggregates().get(KS2_ID)).hasSize(1);
              assertThat(
                      rows.aggregates().get(KS2_ID).iterator().next().getString("aggregate_name"))
                  .isEqualTo("add");
            });
  }

  @Test
  public void should_query_with_paging() {
    SchemaQueriesWithMockedChannel queries =
        new SchemaQueriesWithMockedChannel(driverChannel, node, config, "test");
    CompletionStage<SchemaRows> result = queries.execute();

    // Keyspace
    Call call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.keyspaces");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks1")));

    // No types
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.types");
    call.result.complete(mockResult(/*empty*/ ));

    // Tables
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.tables");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks1", "table_name", "foo")));

    // Columns: paged
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.columns");

    AdminResult page2 =
        mockResult(mockRow("keyspace_name", "ks1", "table_name", "foo", "column_name", "v"));
    AdminResult page1 =
        mockResult(page2, mockRow("keyspace_name", "ks1", "table_name", "foo", "column_name", "k"));
    call.result.complete(page1);

    // No indexes
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.indexes");
    call.result.complete(mockResult(/*empty*/ ));

    // No views
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.views");
    call.result.complete(mockResult(/*empty*/ ));

    // No functions
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.functions");
    call.result.complete(mockResult(/*empty*/ ));

    // No aggregates
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.aggregates");
    call.result.complete(mockResult(/*empty*/ ));

    channel.runPendingTasks();

    assertThatStage(result)
        .isSuccess(
            rows -> {
              assertThat(rows.columns().keySet()).containsOnly(KS1_ID);
              assertThat(rows.columns().get(KS1_ID).keySet()).containsOnly(FOO_ID);
              assertThat(rows.columns().get(KS1_ID).get(FOO_ID))
                  .extracting(r -> r.getString("column_name"))
                  .containsExactly("k", "v");
            });
  }

  @Test
  public void should_ignore_malformed_rows() {
    SchemaQueriesWithMockedChannel queries =
        new SchemaQueriesWithMockedChannel(driverChannel, node, config, "test");
    CompletionStage<SchemaRows> result = queries.execute();

    // Keyspace
    Call call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.keyspaces");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks1")));

    // No types
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.types");
    call.result.complete(mockResult(/*empty*/ ));

    // Tables
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.tables");
    call.result.complete(
        mockResult(
            mockRow("keyspace_name", "ks", "table_name", "foo"),
            // Missing keyspace name:
            mockRow("table_name", "foo")));

    // Columns
    call = queries.calls.poll();
    call.result.complete(
        mockResult(
            mockRow("keyspace_name", "ks", "table_name", "foo", "column_name", "k"),
            // Missing keyspace name:
            mockRow("table_name", "foo", "column_name", "k"),
            // Missing table name:
            mockRow("keyspace_name", "ks", "column_name", "k")));

    AdminResult page2 =
        mockResult(mockRow("keyspace_name", "ks1", "table_name", "foo", "column_name", "v"));
    AdminResult page1 =
        mockResult(page2, mockRow("keyspace_name", "ks1", "table_name", "foo", "column_name", "k"));
    call.result.complete(page1);

    // No indexes
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.indexes");
    call.result.complete(mockResult(/*empty*/ ));

    // No views
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.views");
    call.result.complete(mockResult(/*empty*/ ));

    // No functions
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.functions");
    call.result.complete(mockResult(/*empty*/ ));

    // No aggregates
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.aggregates");
    call.result.complete(mockResult(/*empty*/ ));

    channel.runPendingTasks();

    assertThatStage(result)
        .isSuccess(
            rows -> {
              assertThat(rows.tables().keySet()).containsOnly(KS_ID);
              assertThat(rows.tables().get(KS_ID)).hasSize(1);
              assertThat(rows.tables().get(KS_ID).iterator().next().getString("table_name"))
                  .isEqualTo("foo");

              assertThat(rows.columns().keySet()).containsOnly(KS_ID);
              assertThat(rows.columns().get(KS_ID).keySet()).containsOnly(FOO_ID);
              assertThat(
                      rows.columns()
                          .get(KS_ID)
                          .get(FOO_ID)
                          .iterator()
                          .next()
                          .getString("column_name"))
                  .isEqualTo("k");
            });
  }

  @Test
  public void should_abort_if_query_fails() {
    SchemaQueriesWithMockedChannel queries =
        new SchemaQueriesWithMockedChannel(driverChannel, node, config, "test");
    CompletionStage<SchemaRows> result = queries.execute();

    Exception mockQueryError = new Exception("mock query error");

    Call call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system_schema.keyspaces");
    call.result.completeExceptionally(mockQueryError);

    channel.runPendingTasks();

    assertThatStage(result).isFailed(throwable -> assertThat(throwable).isEqualTo(mockQueryError));
  }

  /** Extends the class under test to mock the query execution logic. */
  static class SchemaQueriesWithMockedChannel extends Cassandra3SchemaQueries {

    final Queue<Call> calls = new LinkedBlockingDeque<>();
    final boolean shouldApplyUsingTimeout;

    SchemaQueriesWithMockedChannel(
        DriverChannel channel, Node node, DriverExecutionProfile config, String logPrefix) {
      this(channel, node, config, logPrefix, false);
    }

    SchemaQueriesWithMockedChannel(
        DriverChannel channel,
        Node node,
        DriverExecutionProfile config,
        String logPrefix,
        boolean shouldApplyUsingTimeout) {
      super(channel, node, config, logPrefix);
      this.shouldApplyUsingTimeout = shouldApplyUsingTimeout;
    }

    @Override
    protected boolean shouldApplyUsingTimeout() {
      return shouldApplyUsingTimeout;
    }

    @Override
    protected CompletionStage<AdminResult> query(String query) {
      Call call = new Call(query);
      calls.add(call);
      return call.result;
    }
  }
}
