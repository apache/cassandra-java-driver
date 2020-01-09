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

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingDeque;
import org.junit.Test;

// Note: we don't repeat the other tests in Cassandra3SchemaQueriesTest because the logic is
// shared, this class just validates the query strings.
public class Cassandra22SchemaQueriesTest extends SchemaQueriesTest {

  @Test
  public void should_query() {
    when(config.getStringList(
            DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, Collections.emptyList()))
        .thenReturn(Collections.emptyList());
    when(node.getCassandraVersion()).thenReturn(Version.V2_2_0);

    SchemaQueriesWithMockedChannel queries =
        new SchemaQueriesWithMockedChannel(driverChannel, node, null, config, "test");

    CompletionStage<SchemaRows> result = queries.execute();

    // Keyspace
    Call call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system.schema_keyspaces");
    call.result.complete(
        mockResult(mockRow("keyspace_name", "ks1"), mockRow("keyspace_name", "ks2")));

    // Types
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system.schema_usertypes");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks1", "type_name", "type")));

    // Tables
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system.schema_columnfamilies");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks1", "columnfamily_name", "foo")));

    // Columns
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system.schema_columns");
    call.result.complete(
        mockResult(
            mockRow("keyspace_name", "ks1", "columnfamily_name", "foo", "column_name", "k")));

    // Functions
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system.schema_functions");
    call.result.complete(mockResult(mockRow("keyspace_name", "ks2", "function_name", "add")));

    // Aggregates
    call = queries.calls.poll();
    assertThat(call.query).isEqualTo("SELECT * FROM system.schema_aggregates");
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
              assertThat(rows.tables().get(KS1_ID).iterator().next().getString("columnfamily_name"))
                  .isEqualTo("foo");

              // Rows
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

              // No views in this version
              assertThat(rows.views().keySet()).isEmpty();
            });
  }

  /** Extends the class under test to mock the query execution logic. */
  static class SchemaQueriesWithMockedChannel extends Cassandra22SchemaQueries {

    final Queue<Call> calls = new LinkedBlockingDeque<>();

    SchemaQueriesWithMockedChannel(
        DriverChannel channel,
        Node node,
        CompletableFuture<Metadata> refreshFuture,
        DriverExecutionProfile config,
        String logPrefix) {
      super(channel, node, config, logPrefix);
    }

    @Override
    protected CompletionStage<AdminResult> query(String query) {
      Call call = new Call(query);
      calls.add(call);
      return call.result;
    }
  }
}
