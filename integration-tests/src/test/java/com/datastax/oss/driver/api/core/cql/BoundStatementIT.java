/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.assertj.core.api.Assertions.assertThat;

public class BoundStatementIT {

  @Rule public CcmRule ccm = CcmRule.getInstance();

  @Rule public ClusterRule cluster = new ClusterRule(ccm, "request.page-size = 20");

  @Rule public TestName name = new TestName();

  private static final int VALUE = 7;

  @Before
  public void setupSchema() {
    // table with simple primary key, single cell.
    cluster
        .session()
        .execute(
            SimpleStatement.builder("CREATE TABLE IF NOT EXISTS test2 (k text primary key, v0 int)")
                .withConfigProfile(cluster.slowProfile())
                .build());
  }

  @Test(expected = IllegalStateException.class)
  @Ignore
  public void should_not_allow_unset_value_on_bound_statement_when_protocol_less_than_v4() {
    // TODO reenable this if JAVA-1584 is fixed.
    try (Cluster v3Cluster = ClusterUtils.newCluster(ccm, "protocol.version = V3")) {
      CqlIdentifier keyspace = ClusterUtils.uniqueKeyspaceId();
      DriverConfigProfile slowProfile = ClusterUtils.slowProfile(v3Cluster);
      ClusterUtils.createKeyspace(v3Cluster, keyspace, slowProfile);
      Session session = v3Cluster.connect(keyspace);
      PreparedStatement prepared =
          session.prepare("INSERT INTO test2 (k, v0, v1) values (?, ?, ?)");

      BoundStatement boundStatement =
          prepared
              .boundStatementBuilder()
              .setString(0, name.getMethodName())
              .unset(1)
              .setString(2, name.getMethodName())
              .build();

      session.execute(boundStatement);
      ClusterUtils.dropKeyspace(v3Cluster, keyspace, slowProfile);
    }
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_not_write_tombstone_if_value_is_implicitly_unset() {
    PreparedStatement prepared =
        cluster.session().prepare("INSERT INTO test2 (k, v0) values (?, ?)");

    cluster.session().execute(prepared.bind(name.getMethodName(), VALUE));

    BoundStatement boundStatement =
        prepared.boundStatementBuilder().setString(0, name.getMethodName()).build();

    verifyUnset(boundStatement);
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_write_tombstone_if_value_is_explicitly_unset() {
    PreparedStatement prepared =
        cluster.session().prepare("INSERT INTO test2 (k, v0) values (?, ?)");

    cluster.session().execute(prepared.bind(name.getMethodName(), VALUE));

    BoundStatement boundStatement =
        prepared
            .boundStatementBuilder()
            .setString(0, name.getMethodName())
            .setInt(1, VALUE + 1) // set initially, will be unset later
            .build();

    verifyUnset(boundStatement.unset(1));
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_write_tombstone_if_value_is_explicitly_unset_on_builder() {
    PreparedStatement prepared =
        cluster.session().prepare("INSERT INTO test2 (k, v0) values (?, ?)");

    cluster.session().execute(prepared.bind(name.getMethodName(), VALUE));

    BoundStatement boundStatement =
        prepared
            .boundStatementBuilder()
            .setString(0, name.getMethodName())
            .setInt(1, VALUE + 1) // set initially, will be unset later
            .unset(1)
            .build();

    verifyUnset(boundStatement);
  }

  private void verifyUnset(BoundStatement boundStatement) {
    cluster.session().execute(boundStatement.unset(1));

    // Verify that no tombstone was written by reading data back and ensuring initial value is retained.
    ResultSet result =
        cluster
            .session()
            .execute(
                SimpleStatement.builder("SELECT v0 from test2 where k = ?")
                    .addPositionalValue(name.getMethodName())
                    .build());

    Row row = result.iterator().next();
    assertThat(row.getInt(0)).isEqualTo(VALUE);
  }
}
