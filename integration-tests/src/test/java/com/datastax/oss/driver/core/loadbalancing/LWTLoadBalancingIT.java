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

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.core.loadbalancing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.testinfra.ScyllaRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@ScyllaRequirement(
    minEnterprise = "2021.0.0",
    minOSS = "4.3.rc0",
    description = "Requires LWT_ADD_METADATA_MARK extension")
public class LWTLoadBalancingIT {
  private static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withNodes(3).build();

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE).withKeyspace(false).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @BeforeClass
  public static void setup() {
    CqlIdentifier keyspace = SessionUtils.uniqueKeyspaceId();
    CqlSession session = SESSION_RULE.session();
    session.execute(
        "CREATE KEYSPACE "
            + keyspace.asCql(false)
            + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
    session.execute("USE " + keyspace.asCql(false));
    session.execute("CREATE TABLE foo (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
  }

  @Test
  public void should_use_only_one_node_when_lwt_detected() {
    assumeTrue(CcmBridge.SCYLLA_ENABLEMENT); // Functionality only available in Scylla
    CqlSession session = SESSION_RULE.session();
    int pk = 1234;
    ByteBuffer routingKey = TypeCodecs.INT.encodePrimitive(pk, ProtocolVersion.DEFAULT);
    TokenMap tokenMap = SESSION_RULE.session().getMetadata().getTokenMap().get();
    Node owner = tokenMap.getReplicas(session.getKeyspace().get(), routingKey).iterator().next();
    PreparedStatement statement =
        SESSION_RULE
            .session()
            .prepare("INSERT INTO foo (pk, ck, v) VALUES (?, ?, ?) IF NOT EXISTS");
    assertThat(statement.isLWT()).isTrue();
    for (int i = 0; i < 30; i++) {
      ResultSet result = session.execute(statement.bind(pk, i, 123));
      assertThat(result.getExecutionInfo().getCoordinator()).isEqualTo(owner);
    }
  }

  @Test
  // Sanity check for the previous test - non-LWT queries should
  // not always be sent to same node
  public void should_not_use_only_one_node_when_non_lwt() {
    assumeTrue(CcmBridge.SCYLLA_ENABLEMENT);
    CqlSession session = SESSION_RULE.session();
    int pk = 1234;
    PreparedStatement statement = session.prepare("INSERT INTO foo (pk, ck, v) VALUES (?, ?, ?)");
    assertThat(statement.isLWT()).isFalse();
    Set<Node> coordinators = new HashSet<>();
    for (int i = 0; i < 30; i++) {
      ResultSet result = session.execute(statement.bind(pk, i, 123));
      coordinators.add(result.getExecutionInfo().getCoordinator());
    }

    // Because keyspace RF == 3
    assertThat(coordinators.size()).isEqualTo(3);
  }
}
