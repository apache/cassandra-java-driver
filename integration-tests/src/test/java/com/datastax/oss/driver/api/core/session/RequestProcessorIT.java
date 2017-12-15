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
package com.datastax.oss.driver.api.core.session;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.example.guava.api.GuavaClusterUtils;
import com.datastax.oss.driver.example.guava.api.GuavaSession;
import com.datastax.oss.driver.example.guava.internal.DefaultGuavaCluster;
import com.datastax.oss.driver.example.guava.internal.GuavaDriverContext;
import com.datastax.oss.driver.example.guava.internal.KeyRequest;
import com.datastax.oss.driver.example.guava.internal.KeyRequestProcessor;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.testinfra.cluster.TestConfigLoader;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A suite of tests for exercising registration of custom {@link
 * com.datastax.oss.driver.internal.core.session.RequestProcessor} implementations to add-in
 * additional request handling and response types.
 *
 * <p>Uses {@link DefaultGuavaCluster} which is a specialized cluster implementation that uses
 * {@link GuavaDriverContext} which overrides {@link
 * DefaultDriverContext#requestProcessorRegistry()} to provide its own {@link
 * com.datastax.oss.driver.internal.core.session.RequestProcessor} implementations for returning
 * {@link ListenableFuture}s rather than {@link java.util.concurrent.CompletionStage}s in async
 * method responses.
 *
 * <p>{@link GuavaSession} provides execute method implementation shortcuts that mimics {@link
 * CqlSession}'s async methods.
 *
 * <p>{@link KeyRequestProcessor} is also registered for handling {@link KeyRequest}s which
 * simplifies a certain query down to 1 parameter.
 */
@Category(ParallelizableTests.class)
public class RequestProcessorIT {

  @ClassRule public static CcmRule ccm = CcmRule.getInstance();

  @ClassRule public static ClusterRule cluster = new ClusterRule(ccm);

  @Rule public ExpectedException thrown = ExpectedException.none();

  public static final String KEY = "test";

  @BeforeClass
  public static void setupSchema() {
    // table with clustering key where v1 == v0 * 2.
    cluster
        .session()
        .execute(
            SimpleStatement.builder(
                    "CREATE TABLE IF NOT EXISTS test (k text, v0 int, v1 int, PRIMARY KEY(k, v0))")
                .withConfigProfile(cluster.slowProfile())
                .build());
    for (int i = 0; i < 100; i++) {
      cluster
          .session()
          .execute(
              SimpleStatement.builder("INSERT INTO test (k, v0, v1) VALUES (?, ?, ?)")
                  .addPositionalValues(KEY, i, i * 2)
                  .build());
    }
  }

  private Cluster<GuavaSession> newCluster(String... options) {
    return GuavaClusterUtils.builder()
        .addContactPoints(ccm.getContactPoints())
        .withConfigLoader(new TestConfigLoader(options))
        .build();
  }

  @Test
  public void should_use_custom_request_processor_for_prepareAsync() throws Exception {
    try (Cluster<GuavaSession> gCluster = newCluster()) {
      GuavaSession session = gCluster.connect(cluster.keyspace());
      ListenableFuture<PreparedStatement> preparedFuture =
          session.prepareAsync("select * from test");

      PreparedStatement prepared = Uninterruptibles.getUninterruptibly(preparedFuture);

      assertThat(prepared.getResultSetDefinitions().contains("k")).isTrue();
      assertThat(prepared.getResultSetDefinitions().contains("v0")).isTrue();
      assertThat(prepared.getResultSetDefinitions().contains("v1")).isTrue();

      ListenableFuture<AsyncResultSet> future = session.executeAsync(prepared.bind());
      AsyncResultSet result = Uninterruptibles.getUninterruptibly(future);
      assertThat(Iterables.size(result.currentPage())).isEqualTo(100);
    }
  }

  @Test
  public void should_use_custom_request_processor_for_handling_special_request_type()
      throws Exception {
    try (Cluster<GuavaSession> gCluster = newCluster()) {
      GuavaSession session = gCluster.connect(cluster.keyspace());

      // RequestProcessor executes "select v from test where k = <KEY>" and returns v as Integer.
      int v1 = session.execute(new KeyRequest(5), KeyRequestProcessor.INT_TYPE);
      assertThat(v1).isEqualTo(10); // v1 = v0 * 2

      // RequestProcessor returns Integer.MIN_VALUE if key not found in data (no rows in result).
      v1 = session.execute(new KeyRequest(200), KeyRequestProcessor.INT_TYPE);
      assertThat(v1).isEqualTo(Integer.MIN_VALUE);
    }
  }

  @Test
  public void should_use_custom_request_processor_for_executeAsync() throws Exception {
    try (Cluster<GuavaSession> gCluster = newCluster()) {
      GuavaSession session = gCluster.connect(cluster.keyspace());

      ListenableFuture<AsyncResultSet> future = session.executeAsync("select * from test");
      AsyncResultSet result = Uninterruptibles.getUninterruptibly(future);
      assertThat(Iterables.size(result.currentPage())).isEqualTo(100);
    }
  }

  @Test
  public void should_throw_illegal_argument_exception_if_no_matching_processor_found()
      throws Exception {
    // Since cluster does not have a processor registered for returning ListenableFuture, an IllegalArgumentException
    // should be thrown.
    thrown.expect(IllegalArgumentException.class);
    cluster
        .session()
        .execute(SimpleStatement.newInstance("select * from test"), GuavaSession.ASYNC);
  }
}
