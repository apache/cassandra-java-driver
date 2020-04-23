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
package com.datastax.oss.driver.core.metadata;

import static com.datastax.oss.driver.api.core.ConsistencyLevel.QUORUM;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.diagnostic.Status;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TokenRingDiagnostic;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultReplicationStrategyFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class DiagnosticsIT {

  private static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(3));

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(SIMULACRON_RULE)
          .withConfigLoader(
              DriverConfigLoader.programmaticBuilder()
                  .withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, true)
                  .withBoolean(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED, true)
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(SIMULACRON_RULE).around(SESSION_RULE);

  private static final ImmutableMap<String, String> KEYSPACE_COLUMNS =
      ImmutableMap.of(
          "keyspace_name", "varchar",
          "durable_writes", "boolean",
          "replication", "map<varchar, varchar>");

  @BeforeClass
  public static void primeSchema() {
    primeKeyspaces(SIMULACRON_RULE.cluster());
  }

  @Test
  public void should_generate_diagnostic_for_simple_strategy() {
    CqlSession session = SESSION_RULE.session();
    Metadata metadata = session.refreshSchema();
    Optional<KeyspaceMetadata> maybeKs = metadata.getKeyspace("ks_simple");
    assertThat(maybeKs).isPresent();
    TokenRingDiagnostic diagnostic =
        metadata.generateTokenRingDiagnostic(
            maybeKs.get().getName(), ConsistencyLevel.QUORUM, "dc1");
    assertThat(diagnostic.getStatus()).isEqualTo(Status.AVAILABLE);
    assertThat(diagnostic.getDetails())
        .isEqualTo(
            ImmutableMap.<String, Object>builder()
                .put("status", Status.AVAILABLE)
                .put("keyspace", "ks_simple")
                .put("replication", maybeKs.get().getReplication())
                .put("consistencyLevel", QUORUM)
                .put("availableRanges", 3)
                .put("unavailableRanges", 0)
                .build());
  }

  @Test
  public void should_generate_diagnostic_for_network_topology_strategy() {
    CqlSession session = SESSION_RULE.session();
    Metadata metadata = session.refreshSchema();
    Optional<KeyspaceMetadata> maybeKs = metadata.getKeyspace("ks_nts");
    assertThat(maybeKs).isPresent();
    TokenRingDiagnostic diagnostic =
        metadata.generateTokenRingDiagnostic(
            maybeKs.get().getName(), ConsistencyLevel.LOCAL_QUORUM, "dc1");
    assertThat(diagnostic.getStatus()).isEqualTo(Status.AVAILABLE);
    assertThat(diagnostic.getDetails())
        .isEqualTo(
            ImmutableMap.<String, Object>builder()
                .put("status", Status.AVAILABLE)
                .put("keyspace", "ks_nts")
                .put("datacenter", "dc1")
                .put("replication", maybeKs.get().getReplication())
                .put("consistencyLevel", ConsistencyLevel.LOCAL_QUORUM)
                .put("availableRanges", 3)
                .put("unavailableRanges", 0)
                .build());
  }

  private static void primeKeyspaces(BoundCluster simulacron) {
    List<Map<String, Object>> allKeyspacesRows =
        ImmutableList.of(
            primeKeyspace(
                simulacron,
                "ks_simple",
                ImmutableMap.of(
                    "class",
                    DefaultReplicationStrategyFactory.SIMPLE_STRATEGY,
                    "replication_factor",
                    "3")),
            primeKeyspace(
                simulacron,
                "ks_nts",
                ImmutableMap.of(
                    "class",
                    DefaultReplicationStrategyFactory.NETWORK_TOPOLOGY_STRATEGY,
                    "dc1",
                    "3")));
    Query whenSelectAllKeyspaces = new Query("SELECT * FROM system_schema.keyspaces");
    SuccessResult thenReturnAllKeyspaces = new SuccessResult(allKeyspacesRows, KEYSPACE_COLUMNS);
    RequestPrime primeAllKeyspaces =
        new RequestPrime(whenSelectAllKeyspaces, thenReturnAllKeyspaces);
    simulacron.prime(new Prime(primeAllKeyspaces));
  }

  private static Map<String, Object> primeKeyspace(
      BoundCluster simulacron, String keyspaceName, Map<String, String> replicationOptions) {
    Map<String, Object> keyspaceRow =
        ImmutableMap.of(
            "keyspace_name", keyspaceName,
            "durable_writes", true,
            "replication", replicationOptions);
    Query whenSelectKeyspace =
        new Query(
            "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '" + keyspaceName + "'");
    SuccessResult thenReturnKeyspace =
        new SuccessResult(Collections.singletonList(keyspaceRow), KEYSPACE_COLUMNS);
    RequestPrime primeKeyspace = new RequestPrime(whenSelectKeyspace, thenReturnKeyspace);
    simulacron.prime(new Prime(primeKeyspace));
    return keyspaceRow;
  }
}
