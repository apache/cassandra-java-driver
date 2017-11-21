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
package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;

import static org.assertj.core.api.Assertions.assertThat;

public class SchemaAgreementIT {

  private static CustomCcmRule ccm = CustomCcmRule.builder().withNodes(3).build();
  private static ClusterRule clusterRule =
      ClusterRule.builder(ccm)
          .withOptions(
              "request.timeout = 30s",
              "load-balancing-policy.class = com.datastax.oss.driver.api.testinfra.loadbalancing.SortingLoadBalancingPolicy",
              "connection.control-connection.schema-agreement.timeout = 3s")
          .build();

  @ClassRule public static RuleChain ruleChain = RuleChain.outerRule(ccm).around(clusterRule);

  @Rule public TestName name = new TestName();

  @Test
  public void should_succeed_when_all_nodes_agree() {
    ResultSet result = createTable();

    assertThat(result.getExecutionInfo().isSchemaInAgreement()).isTrue();
    assertThat(clusterRule.cluster().checkSchemaAgreement()).isTrue();
  }

  @Test
  public void should_fail_on_timeout() {
    ccm.getCcmBridge().pause(2);
    try {
      // Can't possibly agree since one node is paused.
      ResultSet result = createTable();

      assertThat(result.getExecutionInfo().isSchemaInAgreement()).isFalse();
      assertThat(clusterRule.cluster().checkSchemaAgreement()).isFalse();
    } finally {
      ccm.getCcmBridge().resume(2);
    }
  }

  @Test
  public void should_agree_when_up_nodes_agree() {
    ccm.getCcmBridge().stop(2);
    try {
      // Should agree since up hosts should agree.
      ResultSet result = createTable();

      assertThat(result.getExecutionInfo().isSchemaInAgreement()).isTrue();
      assertThat(clusterRule.cluster().checkSchemaAgreement()).isTrue();
    } finally {
      ccm.getCcmBridge().start(2);
    }
  }

  @Test
  public void should_fail_if_timeout_is_zero() {
    try (Cluster<CqlSession> cluster =
        ClusterUtils.newCluster(
            ccm,
            "request.timeout = 30s",
            "connection.control-connection.schema-agreement.timeout = 0s")) {
      CqlSession session = cluster.connect(clusterRule.keyspace());
      ResultSet result = createTable(session);

      // Should not agree because schema metadata is disabled
      assertThat(result.getExecutionInfo().isSchemaInAgreement()).isFalse();
      assertThat(cluster.checkSchemaAgreement()).isFalse();
    }
  }

  private ResultSet createTable() {
    return createTable(clusterRule.session());
  }

  private final AtomicInteger tableCounter = new AtomicInteger();

  private ResultSet createTable(CqlSession session) {
    String tableName = name.getMethodName();
    if (tableName.length() > 48) {
      tableName = tableName.substring(0, 44) + tableCounter.getAndIncrement();
    }
    return session.execute(String.format("CREATE TABLE %s (k int primary key, v int)", tableName));
  }
}
