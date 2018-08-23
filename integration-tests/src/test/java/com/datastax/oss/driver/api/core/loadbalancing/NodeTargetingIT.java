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

package com.datastax.oss.driver.api.core.loadbalancing;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.unavailable;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class NodeTargetingIT {

  @Rule public SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(5));

  @Rule public SessionRule<CqlSession> sessionRule = SessionRule.builder(simulacron).build();

  @Before
  public void clear() {
    simulacron.cluster().clearLogs();
    simulacron.cluster().clearPrimes(true);
  }

  @Test
  public void should_use_node_on_statement() {
    Collection<Node> nodeCol = sessionRule.session().getMetadata().getNodes().values();
    List<Node> nodes = new ArrayList<>(nodeCol);
    for (int i = 0; i < 10; i++) {
      int hostIndex = i % 4 + 1;
      Node node = nodes.get(hostIndex);

      // given a statement with host explicitly set.
      Statement statement = SimpleStatement.newInstance("select * system.local").setNode(node);

      // when statement is executed
      ResultSet result = sessionRule.session().execute(statement);

      // then the query should have been sent to the configured host.
      assertThat(result.getExecutionInfo().getCoordinator()).isEqualTo(node);
    }
  }

  @Test
  public void should_fail_if_host_fails_query() {
    String query = "mock";
    Collection<Node> nodeCol = sessionRule.session().getMetadata().getNodes().values();
    List<Node> nodes = new ArrayList<>(nodeCol);
    simulacron.cluster().node(3).prime(when(query).then(unavailable(ConsistencyLevel.ALL, 1, 0)));

    // given a statement with a host configured to fail the given query.
    Node node1 = nodes.get(3);
    Statement statement = SimpleStatement.newInstance(query).setNode(node1);
    // when statement is executed an error should be raised.
    try {
      sessionRule.session().execute(statement);
      fail("Should have thrown NoNodeAvailableException");
    } catch (AllNodesFailedException e) {
      assertThat(e.getErrors().size()).isEqualTo(1);
      assertThat(e.getErrors().get(node1)).isInstanceOf(UnavailableException.class);
    }
  }

  @Test
  public void should_fail_if_host_is_not_connected() {
    // given a statement with host explicitly set that for which we have no active pool.
    simulacron.cluster().node(4).close();
    Collection<Node> nodeCol = sessionRule.session().getMetadata().getNodes().values();
    List<Node> nodes = new ArrayList<>(nodeCol);
    Node node4 = nodes.get(4);
    Statement statement = SimpleStatement.newInstance("select * system.local").setNode(node4);
    try {
      // when statement is executed
      sessionRule.session().execute(statement);
      fail("Query should have failed");
    } catch (NoNodeAvailableException e) {
      assertThat(e.getErrors()).isEmpty();
    }
  }
}
