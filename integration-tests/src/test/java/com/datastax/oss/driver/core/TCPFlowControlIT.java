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
package com.datastax.oss.driver.core;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codahale.metrics.Gauge;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.channel.DefaultWriteCoalescer;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.server.BoundNode;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.awaitility.Awaitility;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IsolatedTests.class)
public class TCPFlowControlIT {
  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(3));

  // this number is calculated empirically to be high enough to saturate the underling TCP buffer
  private static int NUMBER_OF_SUBMITTED_REQUESTS = 2048;

  @Test
  public void should_not_write_more_requests_to_the_socket_after_the_server_paused_reading()
      throws InterruptedException, ExecutionException, TimeoutException {
    byte[] buffer = new byte[10240];
    Arrays.fill(buffer, (byte) 1);
    ByteBuffer buffer10Kb = ByteBuffer.wrap(buffer);

    String queryString =
        String.format("INSERT INTO table1 (id) VALUES (%s)", ByteUtils.toHexString(buffer10Kb));
    Query query = new Query(queryString, emptyList(), emptyMap(), emptyMap());

    SIMULACRON_RULE.cluster().prime(when(query).then(noRows()));

    try (CqlSession session =
        SessionUtils.newSession(SIMULACRON_RULE, SessionUtils.configLoaderBuilder().build())) {
      SIMULACRON_RULE.cluster().pauseRead();

      // The TCP send and receive buffer size depends on the OS
      // We don't know how much data is needed to be flushed in order for OS to signal as full (and
      // Channel#isWritable return false)
      // Send 20Mb+ to each node
      List<CompletionStage<AsyncResultSet>> pendingRequests = new ArrayList<>();
      for (int i = 0; i < NUMBER_OF_SUBMITTED_REQUESTS; i++) {
        pendingRequests.add(session.executeAsync(SimpleStatement.newInstance(queryString)));
      }

      // Assert that there are still requests that haven't been written
      waitForWriteQueueToStabilize(session);
      assertThat(getWriteQueueSize(session)).isGreaterThan(1);

      SIMULACRON_RULE.cluster().resumeRead();

      int numberOfFinished = 0;
      for (CompletionStage<AsyncResultSet> result : pendingRequests) {
        result.toCompletableFuture().get(1, TimeUnit.SECONDS);
        numberOfFinished = numberOfFinished + 1;
      }

      int writeQueueSize = getWriteQueueSize(session);
      assertThat(writeQueueSize).isEqualTo(0);
      assertThat(numberOfFinished).isEqualTo(NUMBER_OF_SUBMITTED_REQUESTS);
    }
  }

  @Test
  public void should_process_requests_successfully_on_non_paused_nodes()
      throws InterruptedException, ExecutionException, TimeoutException {
    byte[] buffer = new byte[10240];
    Arrays.fill(buffer, (byte) 1);
    ByteBuffer buffer10Kb = ByteBuffer.wrap(buffer);

    String queryString =
        String.format("INSERT INTO table1 (id) VALUES (%s)", ByteUtils.toHexString(buffer10Kb));
    Query query = new Query(queryString, emptyList(), emptyMap(), emptyMap());

    SIMULACRON_RULE.cluster().prime(when(query).then(noRows()));

    try (CqlSession session =
        SessionUtils.newSession(
            SIMULACRON_RULE,
            SessionUtils.configLoaderBuilder()
                .withStringList(
                    DefaultDriverOption.METRICS_NODE_ENABLED,
                    Collections.singletonList("pool.in-flight"))
                .build())) {
      int hostIndex = 2;
      BoundNode pausedNode = SIMULACRON_RULE.cluster().node(hostIndex);
      SocketAddress pausedHostAddress = pausedNode.getAddress();
      List<Node> nonPausedNodes =
          session.getMetadata().getNodes().values().stream()
              .filter(n -> !pausedHostAddress.equals(n.getBroadcastRpcAddress().get()))
              .collect(Collectors.toList());
      assertThat(nonPausedNodes.size()).isEqualTo(2);

      SIMULACRON_RULE.cluster().node(2).pauseRead();

      List<CompletionStage<AsyncResultSet>> pendingRequests = new ArrayList<>();
      for (int i = 0; i < NUMBER_OF_SUBMITTED_REQUESTS; i++) {
        pendingRequests.add(session.executeAsync(SimpleStatement.newInstance(queryString)));
      }

      // Non-paused nodes should process the requests correctly
      for (Node n : nonPausedNodes) {
        Awaitility.await()
            .until(
                () -> {
                  int inFlightRequests = getInFlightRequests(session, n);
                  return inFlightRequests == 0;
                });
      }

      // There are still requests that haven't been written
      waitForWriteQueueToStabilize(session);
      assertThat(getWriteQueueSize(session)).isGreaterThan(1);

      SIMULACRON_RULE.cluster().resumeRead();

      int numberOfFinished = 0;
      for (CompletionStage<AsyncResultSet> result : pendingRequests) {
        result.toCompletableFuture().get(1, TimeUnit.SECONDS);
        numberOfFinished = numberOfFinished + 1;
      }

      int writeQueueSize = getWriteQueueSize(session);
      assertThat(writeQueueSize).isEqualTo(0);
      assertThat(numberOfFinished).isEqualTo(NUMBER_OF_SUBMITTED_REQUESTS);
    }
  }

  @Test
  public void should_timeout_requests_when_the_server_paused_reading_without_resumimg()
      throws InterruptedException {
    byte[] buffer = new byte[10240];
    Arrays.fill(buffer, (byte) 1);
    ByteBuffer buffer10Kb = ByteBuffer.wrap(buffer);

    String queryString =
        String.format("INSERT INTO table1 (id) VALUES (%s)", ByteUtils.toHexString(buffer10Kb));
    Query query = new Query(queryString, emptyList(), emptyMap(), emptyMap());

    SIMULACRON_RULE.cluster().prime(when(query).then(noRows()));

    try (CqlSession session =
        SessionUtils.newSession(SIMULACRON_RULE, SessionUtils.configLoaderBuilder().build())) {
      SIMULACRON_RULE.cluster().pauseRead();

      List<CompletionStage<AsyncResultSet>> pendingRequests = new ArrayList<>();
      for (int i = 0; i < NUMBER_OF_SUBMITTED_REQUESTS; i++) {
        pendingRequests.add(session.executeAsync(SimpleStatement.newInstance(queryString)));
      }

      // Assert that there are still requests that haven't been written
      waitForWriteQueueToStabilize(session);
      assertThat(getWriteQueueSize(session)).isGreaterThan(1);

      // do not resumeRead
      for (CompletionStage<AsyncResultSet> result : pendingRequests) {
        assertThatThrownBy(() -> result.toCompletableFuture().get())
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(DriverTimeoutException.class);
      }

      // write queue size is still non empty
      int writeQueueSize = getWriteQueueSize(session);
      assertThat(writeQueueSize).isGreaterThan(1);
      SIMULACRON_RULE.cluster().stop();
    }
  }

  @SuppressWarnings("unchecked")
  private int getInFlightRequests(CqlSession session, Node n) {
    return ((Gauge<Integer>)
            session.getMetrics().get().getNodeMetric(n, DefaultNodeMetric.IN_FLIGHT).get())
        .getValue();
  }

  private Integer getWriteQueueSize(CqlSession session) {
    return ((DefaultWriteCoalescer)
            ((DefaultDriverContext) session.getContext()).getWriteCoalescer())
        .getWriteQueueSize();
  }

  private void waitForWriteQueueToStabilize(CqlSession cqlSession) throws InterruptedException {
    final Integer[] lastWriteQueueValue = {getWriteQueueSize(cqlSession)};
    // initial delay
    Thread.sleep(500);
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .pollDelay(Duration.ofSeconds(1))
        .until(
            () -> {
              Integer currentValue = getWriteQueueSize(cqlSession);
              Integer tmpLastValue = lastWriteQueueValue[0];
              lastWriteQueueValue[0] = currentValue;
              return tmpLastValue.equals(currentValue);
            });
  }
}
