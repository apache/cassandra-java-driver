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

package com.datastax.oss.driver.core.loadbalancing;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.unavailable;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.server.BoundNode;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class NodeTargetingIT {

  private static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(5));

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(SIMULACRON_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(SIMULACRON_RULE).around(SESSION_RULE);

  @Before
  public void clear() {
    SIMULACRON_RULE.cluster().clearLogs();
    SIMULACRON_RULE.cluster().clearPrimes(true);
    SIMULACRON_RULE.cluster().node(4).stop();
    await()
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> getNode(4).getState() == NodeState.DOWN);
  }

  @Test
  public void should_use_node_on_statement() {
    for (int i = 0; i < 10; i++) {
      int nodeIndex = i % 3 + 1;
      Node node = getNode(nodeIndex);

      // given a statement with node explicitly set.
      Statement statement = SimpleStatement.newInstance("select * system.local").setNode(node);

      // when statement is executed
      ResultSet result = SESSION_RULE.session().execute(statement);

      // then the query should have been sent to the configured node.
      assertThat(result.getExecutionInfo().getCoordinator()).isEqualTo(node);
    }
  }

  @Test
  public void should_fail_if_node_fails_query() {
    String query = "mock";
    SIMULACRON_RULE
        .cluster()
        .node(3)
        .prime(when(query).then(unavailable(ConsistencyLevel.ALL, 1, 0)));

    // given a statement with a node configured to fail the given query.
    Node node3 = getNode(3);
    Statement statement = SimpleStatement.newInstance(query).setNode(node3);
    // when statement is executed an error should be raised.
    try {
      SESSION_RULE.session().execute(statement);
      fail("Should have thrown AllNodesFailedException");
    } catch (AllNodesFailedException e) {
      assertThat(e.getAllErrors().size()).isEqualTo(1);
      assertThat(e.getAllErrors().get(node3).get(0)).isInstanceOf(UnavailableException.class);
    }
  }

  @Test
  public void should_fail_if_node_is_not_connected() {
    // given a statement with node explicitly set that for which we have no active pool.
    Node node4 = getNode(4);

    Statement statement = SimpleStatement.newInstance("select * system.local").setNode(node4);
    try {
      // when statement is executed
      SESSION_RULE.session().execute(statement);
      fail("Query should have failed");
    } catch (NoNodeAvailableException e) {
      assertThat(e.getAllErrors()).isEmpty();
    } catch (AllNodesFailedException e) {
      // its also possible that the query is tried.  This can happen if the node was marked
      // down, but not all connections have been closed yet.  In this case, just verify that
      // the expected host failed.
      assertThat(e.getAllErrors().size()).isEqualTo(1);
      assertThat(e.getAllErrors()).containsOnlyKeys(node4);
    }
  }

  private Node getNode(int id) {
    BoundNode boundNode = SIMULACRON_RULE.cluster().node(id);
    assertThat(boundNode).isNotNull();
    InetSocketAddress address = (InetSocketAddress) boundNode.getAddress();
    return SESSION_RULE
        .session()
        .getMetadata()
        .findNode(address)
        .orElseThrow(
            () -> new AssertionError(String.format("Expected to find node %d in metadata", id)));
  }
}
