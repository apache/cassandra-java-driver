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
package com.datastax.oss.driver.core.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.tracker.RequestLogFormatter;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
@Category(ParallelizableTests.class)
public class PagingIterableSpliteratorIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withClassList(
                      DefaultDriverOption.REQUEST_TRACKER_CLASSES,
                      Collections.singletonList(RecordingRequestTracker.class))
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @BeforeClass
  public static void setupSchema() {
    SESSION_RULE
        .session()
        .execute(
            SimpleStatement.builder(
                    "CREATE TABLE IF NOT EXISTS test (k0 int, k1 int, v int, PRIMARY KEY(k0, k1))")
                .setExecutionProfile(SESSION_RULE.slowProfile())
                .build());
    PreparedStatement prepared =
        SESSION_RULE.session().prepare("INSERT INTO test (k0, k1, v) VALUES (?, ?, ?)");
    for (int i = 0; i < 20_000; i += 1_000) {
      BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
      for (int j = 0; j < 1_000; j++) {
        int n = i + j;
        batch.addStatement(prepared.bind(0, n, n));
      }
      SESSION_RULE.session().execute(batch.setExecutionProfile(SESSION_RULE.slowProfile()).build());
    }
    RecordingRequestTracker.reset();
  }

  @Test
  public void should_notify_request_tracker_during_pagination() throws Exception {
    String query = "SELECT v FROM test where k0 = 0";
    RecordingRequestTracker.query = query;
    CqlSession session = SESSION_RULE.session();
    ResultSet result = session.execute(SimpleStatement.newInstance(query));
    Iterator<Row> iterator = result.iterator();
    while (iterator.hasNext()) {
      Row row = iterator.next();
      assertThat(row.getInt("v")).isGreaterThanOrEqualTo(0);
    }
    int expectedFetches = 20_000 / 5_000 + 1; // +1 to retrieve empty page
    assertThat(RecordingRequestTracker.startedRequests).hasSize(expectedFetches);
    assertThat(RecordingRequestTracker.startedRequestsAtNode).hasSize(expectedFetches);
    assertThat(RecordingRequestTracker.successfulRequestsAtNode).hasSize(expectedFetches);
    assertThat(RecordingRequestTracker.successfulRequests).hasSize(expectedFetches);
    assertThat(RecordingRequestTracker.errorRequestsAtNode).hasSize(0);
    assertThat(RecordingRequestTracker.errorRequests).hasSize(0);
  }

  @Test
  @UseDataProvider("pageSizes")
  public void should_consume_spliterator(int pageSize, boolean parallel) throws Exception {
    CqlSession session = SESSION_RULE.session();
    DriverExecutionProfile profile =
        session
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, pageSize);
    ResultSet result =
        session.execute(
            SimpleStatement.newInstance("SELECT v FROM test where k0 = 0")
                .setExecutionProfile(profile));
    Spliterator<Row> spliterator = result.spliterator();
    if (pageSize > 20_000) {
      // if the page size is greater than the result set size,
      // we create a SinglePageResultSet with known spliterator size
      assertThat(spliterator.estimateSize()).isEqualTo(20_000);
      assertThat(spliterator.getExactSizeIfKnown()).isEqualTo(20_000);
      assertThat(spliterator.characteristics())
          .isEqualTo(
              Spliterator.ORDERED
                  | Spliterator.IMMUTABLE
                  | Spliterator.NONNULL
                  | Spliterator.SIZED
                  | Spliterator.SUBSIZED);
    } else {
      assertThat(spliterator.estimateSize()).isEqualTo(Long.MAX_VALUE);
      assertThat(spliterator.getExactSizeIfKnown()).isEqualTo(-1);
      assertThat(spliterator.characteristics())
          .isEqualTo(Spliterator.ORDERED | Spliterator.IMMUTABLE | Spliterator.NONNULL);
    }
    long count = StreamSupport.stream(spliterator, parallel).count();
    assertThat(count).isEqualTo(20_000L);
  }

  @DataProvider
  public static Iterable<?> pageSizes() {
    List<List<Object>> arguments = new ArrayList<>();
    arguments.add(Lists.newArrayList(30_000, false));
    arguments.add(Lists.newArrayList(20_000, false));
    arguments.add(Lists.newArrayList(10_000, false));
    arguments.add(Lists.newArrayList(5_000, false));
    arguments.add(Lists.newArrayList(500, false));
    arguments.add(Lists.newArrayList(9_999, false));
    arguments.add(Lists.newArrayList(10_001, false));
    arguments.add(Lists.newArrayList(5, false));
    arguments.add(Lists.newArrayList(19_995, false));
    arguments.add(Lists.newArrayList(30_000, true));
    arguments.add(Lists.newArrayList(20_000, true));
    arguments.add(Lists.newArrayList(10_000, true));
    arguments.add(Lists.newArrayList(5_000, true));
    arguments.add(Lists.newArrayList(500, true));
    arguments.add(Lists.newArrayList(9_999, true));
    arguments.add(Lists.newArrayList(10_001, true));
    arguments.add(Lists.newArrayList(5, true));
    arguments.add(Lists.newArrayList(19_995, true));
    return arguments;
  }

  public static class RecordingRequestTracker implements RequestTracker {

    private static volatile String query = "none";
    private static final List<Request> startedRequests = new ArrayList<>();
    private static final List<Pair<Request, Node>> startedRequestsAtNode = new ArrayList<>();
    private static final List<Pair<Request, Node>> successfulRequestsAtNode = new ArrayList<>();
    private static final List<Request> successfulRequests = new ArrayList<>();
    private static final List<Pair<Request, Node>> errorRequestsAtNode = new ArrayList<>();
    private static final List<Request> errorRequests = new ArrayList<>();

    private final RequestLogFormatter formatter;

    public RecordingRequestTracker(DriverContext context) {
      formatter = new RequestLogFormatter(context);
    }

    @Override
    public synchronized void onRequestCreated(
        @NonNull Request request,
        @NonNull DriverExecutionProfile executionProfile,
        @NonNull String requestLogPrefix) {
      if (shouldRecord(request)) {
        startedRequests.add(request);
      }
    }

    @Override
    public synchronized void onRequestCreatedForNode(
        @NonNull Request request,
        @NonNull DriverExecutionProfile executionProfile,
        @NonNull Node node,
        @NonNull String requestLogPrefix) {
      if (shouldRecord(request)) {
        startedRequestsAtNode.add(Pair.of(request, node));
      }
    }

    @Override
    public synchronized void onNodeSuccess(
        @NonNull Request request,
        long latencyNanos,
        @NonNull DriverExecutionProfile executionProfile,
        @NonNull Node node,
        @Nullable ExecutionInfo executionInfo,
        @NonNull String requestLogPrefix) {
      if (shouldRecord(request)) {
        successfulRequestsAtNode.add(Pair.of(request, node));
      }
    }

    @Override
    public synchronized void onSuccess(
        @NonNull Request request,
        long latencyNanos,
        @NonNull DriverExecutionProfile executionProfile,
        @NonNull Node node,
        @Nullable ExecutionInfo executionInfo,
        @NonNull String requestLogPrefix) {
      if (shouldRecord(request)) {
        successfulRequests.add(request);
      }
    }

    @Override
    public synchronized void onNodeError(
        @NonNull Request request,
        @NonNull Throwable error,
        long latencyNanos,
        @NonNull DriverExecutionProfile executionProfile,
        @NonNull Node node,
        @Nullable ExecutionInfo executionInfo,
        @NonNull String requestLogPrefix) {
      if (shouldRecord(request)) {
        errorRequestsAtNode.add(Pair.of(request, node));
      }
    }

    @Override
    public synchronized void onError(
        @NonNull Request request,
        @NonNull Throwable error,
        long latencyNanos,
        @NonNull DriverExecutionProfile executionProfile,
        @Nullable Node node,
        @Nullable ExecutionInfo executionInfo,
        @NonNull String requestLogPrefix) {
      if (shouldRecord(request)) {
        errorRequests.add(request);
      }
    }

    private boolean shouldRecord(Request request) {
      if (query == null) {
        return true;
      }
      StringBuilder builder = new StringBuilder();
      formatter.appendRequest(request, 1000, true, 1000, 1000, builder);
      return builder.toString().contains(query);
    }

    @Override
    public void close() throws Exception {
      reset();
    }

    public static void reset() {
      query = "none";
      startedRequests.clear();
      startedRequestsAtNode.clear();
      successfulRequestsAtNode.clear();
      successfulRequests.clear();
      errorRequestsAtNode.clear();
      errorRequests.clear();
    }
  }
}
