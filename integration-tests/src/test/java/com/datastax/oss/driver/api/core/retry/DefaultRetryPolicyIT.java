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
package com.datastax.oss.driver.api.core.retry;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.stubbing.CloseType;
import com.datastax.oss.simulacron.common.stubbing.DisconnectAction;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Arrays;
import java.util.Map;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.LOCAL_QUORUM;
import static com.datastax.oss.simulacron.common.codec.WriteType.BATCH_LOG;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.closeConnection;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.readTimeout;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.serverError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.unavailable;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.writeTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@Category(ParallelizableTests.class)
@RunWith(DataProviderRunner.class)
public class DefaultRetryPolicyIT {

  public static @ClassRule SimulacronRule simulacron =
      new SimulacronRule(ClusterSpec.builder().withNodes(3));
  public @Rule ClusterRule cluster =
      new ClusterRule(
          simulacron,
          "request.default-idempotence = true",
          "load-balancing-policy.class = com.datastax.oss.driver.api.testinfra.loadbalancing.SortingLoadBalancingPolicy");

  private static String queryStr = "select * from foo";
  private static final SimpleStatement query = SimpleStatement.builder(queryStr).build();

  @Before
  public void clear() {
    // clear activity logs and primes between tests since simulacron instance is shared.
    simulacron.cluster().clearLogs();
    simulacron.cluster().clearPrimes(true);
  }

  private void assertQueryCount(int expected) {
    assertThat(
            simulacron
                .cluster()
                .getLogs()
                .getQueryLogs()
                .stream()
                .filter(l -> l.getQuery().equals(queryStr)))
        .as("Expected query count to be %d", expected)
        .hasSize(expected);
  }

  private void assertQueryCount(int node, int expected) {
    assertThat(
            simulacron
                .cluster()
                .node(node)
                .getLogs()
                .getQueryLogs()
                .stream()
                .filter(l -> l.getQuery().equals(queryStr)))
        .as("Expected query count to be %d for node %d", expected, node)
        .hasSize(expected);
  }

  @Test
  public void should_not_retry_on_read_timeout_when_data_present() {
    // given a node that will respond to query with a read timeout where data is present.
    simulacron.cluster().node(0).prime(when(queryStr).then(readTimeout(LOCAL_QUORUM, 1, 3, true)));

    try {
      // when executing a query
      cluster.session().execute(query);
      fail("Expected a ReadTimeoutException");
    } catch (ReadTimeoutException rte) {
      // then a read timeout exception is thrown
      assertThat(rte.getConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
      assertThat(rte.getReceived()).isEqualTo(1);
      assertThat(rte.getBlockFor()).isEqualTo(3);
      assertThat(rte.wasDataPresent()).isTrue();
    }

    // should not have been retried.
    assertQueryCount(1);
  }

  @Test
  public void should_not_retry_on_read_timeout_when_less_than_blockFor_received() {
    // given a node that will respond to a query with a read timeout where 2 out of 3 responses are received.
    // in this case, digest requests succeeded, but not the data request.
    simulacron.cluster().node(0).prime(when(queryStr).then(readTimeout(LOCAL_QUORUM, 2, 3, false)));

    try {
      // when executing a query
      cluster.session().execute(query);
      fail("Expected a ReadTimeoutException");
    } catch (ReadTimeoutException rte) {
      // then a read timeout exception is thrown
      assertThat(rte.getConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
      assertThat(rte.getReceived()).isEqualTo(2);
      assertThat(rte.getBlockFor()).isEqualTo(3);
      assertThat(rte.wasDataPresent()).isFalse();
    }

    // should not have been retried.
    assertQueryCount(1);
  }

  @Test
  public void should_retry_on_read_timeout_when_enough_responses_and_data_not_present() {
    // given a node that will respond to a query with a read timeout where 3 out of 3 responses are received,
    // but data is not present.
    simulacron.cluster().node(0).prime(when(queryStr).then(readTimeout(LOCAL_QUORUM, 3, 3, false)));

    try {
      // when executing a query.
      cluster.session().execute(query);
      fail("Expected a ReadTimeoutException");
    } catch (ReadTimeoutException rte) {
      // then a read timeout exception is thrown.
      assertThat(rte.getConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
      assertThat(rte.getReceived()).isEqualTo(3);
      assertThat(rte.getBlockFor()).isEqualTo(3);
      assertThat(rte.wasDataPresent()).isFalse();
    }

    // there should have been a retry, and it should have been executed on the same host.
    assertQueryCount(2);
    assertQueryCount(0, 2);
  }

  @Test
  public void should_retry_on_next_host_on_connection_error_if_idempotent() {
    // given a node that will close its connection as result of receiving a query.
    simulacron
        .cluster()
        .node(0)
        .prime(
            when(queryStr)
                .then(closeConnection(DisconnectAction.Scope.CONNECTION, CloseType.DISCONNECT)));

    // when executing a query.
    ResultSet result = cluster.session().execute(query);
    // then we should get a response, and the execution info on the result set indicates there was an error on
    // the host that received the query.
    assertThat(result.getExecutionInfo().getErrors()).hasSize(1);
    Map.Entry<Node, Throwable> error = result.getExecutionInfo().getErrors().get(0);
    assertThat(error.getKey().getConnectAddress())
        .isEqualTo(simulacron.cluster().node(0).inetSocketAddress());
    assertThat(error.getValue()).isInstanceOf(ClosedConnectionException.class);
    // the host that returned the response should be node 1.
    assertThat(result.getExecutionInfo().getCoordinator().getConnectAddress())
        .isEqualTo(simulacron.cluster().node(1).inetSocketAddress());

    // should have been retried.
    assertQueryCount(2);
    // expected query on node 0.
    assertQueryCount(0, 1);
    // expected retry on node 1.
    assertQueryCount(1, 1);
  }

  @Test
  public void should_keep_retrying_on_next_host_on_connection_error() {
    // given a request for which every node will close its connection upon receiving it.
    simulacron
        .cluster()
        .prime(
            when(queryStr)
                .then(closeConnection(DisconnectAction.Scope.CONNECTION, CloseType.DISCONNECT)));

    try {
      // when executing a query.
      cluster.session().execute(query);
      fail("AllNodesFailedException expected");
    } catch (AllNodesFailedException ex) {
      // then an AllNodesFailedException should be raised indicating that all nodes failed the request.
      assertThat(ex.getErrors()).hasSize(3);
    }

    // should have been tried on all nodes.
    assertQueryCount(3);
    // expected query on node 0.
    assertQueryCount(0, 1);
    // expected retry on node 1.
    assertQueryCount(1, 1);
    // expected query on node 2.
    assertQueryCount(2, 1);
  }

  @Test
  public void should_not_retry_on_connection_error_if_non_idempotent() {
    // given a node that will close its connection as result of receiving a query.
    simulacron
        .cluster()
        .node(0)
        .prime(
            when(queryStr)
                .then(closeConnection(DisconnectAction.Scope.CONNECTION, CloseType.DISCONNECT)));

    try {
      // when executing a non-idempotent query.
      cluster.session().execute(SimpleStatement.builder(queryStr).withIdempotence(false).build());
      fail("ClosedConnectionException expected");
    } catch (ClosedConnectionException ex) {
      // then a ClosedConnectionException should be raised, indicating that the connection closed while handling
      // the request on that node.
      // this clearly indicates that the request wasn't retried.
      // Exception should indicate that node 0 was the failing node.
      // TODO: Validate the address on the connection if made available.
    }

    // should not have been retried.
    assertQueryCount(1);
  }

  @Test
  public void should_retry_on_write_timeout_if_write_type_batch_log() {
    // given a node that will respond to query with a write timeout with write type of batch log.
    simulacron
        .cluster()
        .node(0)
        .prime(when(queryStr).then(writeTimeout(LOCAL_QUORUM, 1, 3, BATCH_LOG)));

    try {
      // when executing a query.
      cluster.session().execute(queryStr);
      fail("WriteTimeoutException expected");
    } catch (WriteTimeoutException wte) {
      // then a write timeout exception is thrown
      assertThat(wte.getConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
      assertThat(wte.getReceived()).isEqualTo(1);
      assertThat(wte.getBlockFor()).isEqualTo(3);
      assertThat(wte.getWriteType()).isEqualTo(WriteType.BATCH_LOG);
    }

    // there should have been a retry, and it should have been executed on the same host.
    assertQueryCount(2);
    assertQueryCount(0, 2);
  }

  /**
   * @return All WriteTypes that are not BATCH_LOG, on write timeout of these, the driver should not
   *     retry.
   */
  @DataProvider
  public static Object[] nonBatchLogWriteTypes() {
    return Arrays.stream(com.datastax.oss.simulacron.common.codec.WriteType.values())
        .filter(wt -> wt != BATCH_LOG)
        .toArray();
  }

  @UseDataProvider("nonBatchLogWriteTypes")
  @Test
  public void should_not_retry_on_write_timeout_if_write_type_non_batch_log(
      com.datastax.oss.simulacron.common.codec.WriteType writeType) {
    // given a node that will respond to query with a write timeout with write type that is not batch log.
    simulacron
        .cluster()
        .node(0)
        .prime(when(queryStr).then(writeTimeout(LOCAL_QUORUM, 1, 3, writeType)));

    try {
      // when executing a query.
      cluster.session().execute(queryStr);
      fail("WriteTimeoutException expected");
    } catch (WriteTimeoutException wte) {
      // then a write timeout exception is thrown
      assertThat(wte.getConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
      assertThat(wte.getReceived()).isEqualTo(1);
      assertThat(wte.getBlockFor()).isEqualTo(3);
    }

    // should not have been retried.
    assertQueryCount(1);
  }

  @Test
  public void should_not_retry_on_write_timeout_if_write_type_batch_log_but_non_idempotent() {
    // given a node that will respond to query with a write timeout with write type of batch log.
    simulacron
        .cluster()
        .node(0)
        .prime(when(queryStr).then(writeTimeout(LOCAL_QUORUM, 1, 3, BATCH_LOG)));

    try {
      // when executing a non-idempotent query.
      cluster.session().execute(SimpleStatement.builder(queryStr).withIdempotence(false).build());
      fail("WriteTimeoutException expected");
    } catch (WriteTimeoutException wte) {
      // then a write timeout exception is thrown
      assertThat(wte.getConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
      assertThat(wte.getReceived()).isEqualTo(1);
      assertThat(wte.getBlockFor()).isEqualTo(3);
      assertThat(wte.getWriteType()).isEqualTo(WriteType.BATCH_LOG);
    }

    // should not have been retried.
    assertQueryCount(1);
  }

  @Test
  public void should_retry_on_next_host_on_unavailable() {
    // given a node that will respond to a query with an unavailable.
    simulacron.cluster().node(0).prime(when(queryStr).then(unavailable(LOCAL_QUORUM, 3, 0)));

    // when executing a query.
    ResultSet result = cluster.session().execute(queryStr);
    // then we should get a response, and the execution info on the result set indicates there was an error on
    // the host that received the query.
    assertThat(result.getExecutionInfo().getErrors()).hasSize(1);
    Map.Entry<Node, Throwable> error = result.getExecutionInfo().getErrors().get(0);
    assertThat(error.getKey().getConnectAddress())
        .isEqualTo(simulacron.cluster().node(0).inetSocketAddress());
    assertThat(error.getValue()).isInstanceOf(UnavailableException.class);
    // the host that returned the response should be node 1.
    assertThat(result.getExecutionInfo().getCoordinator().getConnectAddress())
        .isEqualTo(simulacron.cluster().node(1).inetSocketAddress());

    // should have been retried on another host.
    assertQueryCount(2);
    assertQueryCount(0, 1);
    assertQueryCount(1, 1);
  }

  @Test
  public void should_only_retry_once_on_unavailable() {
    // given two nodes that will respond to a query with an unavailable.
    simulacron.cluster().node(0).prime(when(queryStr).then(unavailable(LOCAL_QUORUM, 3, 0)));
    simulacron.cluster().node(1).prime(when(queryStr).then(unavailable(LOCAL_QUORUM, 3, 0)));

    try {
      // when executing a query.
      cluster.session().execute(queryStr);
    } catch (UnavailableException ue) {
      // then we should get an unavailable exception with the host being node 1 (since it was second tried).
      assertThat(ue.getCoordinator().getConnectAddress())
          .isEqualTo(simulacron.cluster().node(1).inetSocketAddress());
      assertThat(ue.getConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
      assertThat(ue.getRequired()).isEqualTo(3);
      assertThat(ue.getAlive()).isEqualTo(0);
    }

    // should have been retried on another host.
    assertQueryCount(2);
    assertQueryCount(0, 1);
    assertQueryCount(1, 1);
  }

  @Test
  public void should_keep_retrying_on_next_host_on_error_response() {
    // given every node responding with a server error.
    simulacron.cluster().prime(when(queryStr).then(serverError("this is a server error")));

    try {
      // when executing a query.
      cluster.session().execute(queryStr);
    } catch (AllNodesFailedException e) {
      // then we should get an all nodes failed exception, indicating the query was tried each node.
      assertThat(e.getErrors()).hasSize(3);
      for (Throwable t : e.getErrors().values()) {
        assertThat(t).isInstanceOf(ServerError.class);
      }
    }

    // should have been tried on all nodes.
    assertQueryCount(3);
    // expected query on node 0.
    assertQueryCount(0, 1);
    // expected retry on node 1.
    assertQueryCount(1, 1);
    // expected query on node 2.
    assertQueryCount(2, 1);
  }

  @Test
  public void should_not_retry_on_next_host_on_error_response_if_non_idempotent() {
    // given every node responding with a server error.
    simulacron.cluster().prime(when(queryStr).then(serverError("this is a server error")));

    try {
      // when executing a query that is not idempotent
      cluster.session().execute(SimpleStatement.builder(queryStr).withIdempotence(false).build());
    } catch (ServerError e) {
      // then should get a server error from first host.
      assertThat(e.getMessage()).isEqualTo("this is a server error");
    }

    // should have been tried on all nodes.
    assertQueryCount(1);
    // expected query on node 0.
    assertQueryCount(0, 1);
  }
}
