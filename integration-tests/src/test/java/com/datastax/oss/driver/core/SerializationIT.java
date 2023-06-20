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

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.serverError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.SerializationHelper;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class SerializationIT {
  private static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(SIMULACRON_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(SIMULACRON_RULE).around(SESSION_RULE);

  @Before
  public void clear() {
    SIMULACRON_RULE.cluster().clearPrimes(true);
  }

  @Test
  public void should_serialize_node() {
    // Given
    Node node = SESSION_RULE.session().getMetadata().getNodes().values().iterator().next();

    // When
    Node deserializedNode = SerializationHelper.serializeAndDeserialize(node);

    // Then
    // verify a few fields, no need to be exhaustive
    assertThat(deserializedNode.getHostId()).isEqualTo(node.getHostId());
    assertThat(deserializedNode.getEndPoint()).isEqualTo(node.getEndPoint());
    assertThat(deserializedNode.getCassandraVersion()).isEqualTo(node.getCassandraVersion());
  }

  @Test
  public void should_serialize_driver_exception() {
    // Given
    SIMULACRON_RULE.cluster().prime(when("mock query").then(serverError("mock server error")));
    try {
      SESSION_RULE.session().execute("mock query");
      fail("Expected a ServerError");
    } catch (ServerError error) {
      assertThat(error.getExecutionInfo()).isNotNull();

      // When
      ServerError deserializedError = SerializationHelper.serializeAndDeserialize(error);

      // Then
      assertThat(deserializedError.getMessage()).isEqualTo("mock server error");
      assertThat(deserializedError.getCoordinator().getEndPoint())
          .isEqualTo(error.getCoordinator().getEndPoint());
      assertThat(deserializedError.getExecutionInfo()).isNull(); // transient
    }
  }

  @Test
  public void should_serialize_row() {
    // Given
    SIMULACRON_RULE
        .cluster()
        .prime(when("mock query").then(rows().row("t", "mock data").columnTypes("t", "varchar")));
    Row row = SESSION_RULE.session().execute("mock query").one();

    // When
    row = SerializationHelper.serializeAndDeserialize(row);

    // Then
    ColumnDefinition columnDefinition = row.getColumnDefinitions().get("t");
    assertThat(columnDefinition.getType()).isEqualTo(DataTypes.TEXT);
    assertThat(row.getString("t")).isEqualTo("mock data");
  }
}
