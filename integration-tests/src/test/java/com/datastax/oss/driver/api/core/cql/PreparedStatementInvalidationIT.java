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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import static junit.framework.TestCase.fail;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Note: at the time of writing, some of these tests exercises features of an unreleased Cassandra
 * version. To test against a local build, run with
 *
 * <pre>
 *   -Dccm.cassandraVersion=4.0.0 -Dccm.cassandraDirectory=/path/to/cassandra -Ddatastax-java-driver.protocol.version=V5
 * </pre>
 */
@Category(ParallelizableTests.class)
public class PreparedStatementInvalidationIT {

  @Rule public CcmRule ccmRule = CcmRule.getInstance();

  @Rule
  public ClusterRule clusterRule =
      new ClusterRule(ccmRule, "request.page-size = 2", "request.timeout = 30 seconds");

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setupSchema() {
    for (String query :
        ImmutableList.of(
            "CREATE TABLE prepared_statement_invalidation_test (a int PRIMARY KEY, b int, c int)",
            "INSERT INTO prepared_statement_invalidation_test (a, b, c) VALUES (1, 1, 1)",
            "INSERT INTO prepared_statement_invalidation_test (a, b, c) VALUES (2, 2, 2)",
            "INSERT INTO prepared_statement_invalidation_test (a, b, c) VALUES (3, 3, 3)",
            "INSERT INTO prepared_statement_invalidation_test (a, b, c) VALUES (4, 4, 4)")) {
      clusterRule
          .session()
          .execute(
              SimpleStatement.builder(query).withConfigProfile(clusterRule.slowProfile()).build());
    }
  }

  @Test
  @CassandraRequirement(min = "4.0")
  public void should_update_metadata_when_schema_changed_across_executions() {
    // Given
    CqlSession session = clusterRule.session();
    PreparedStatement ps =
        session.prepare("SELECT * FROM prepared_statement_invalidation_test WHERE a = ?");
    ByteBuffer idBefore = ps.getResultMetadataId();

    // When
    session.execute(
        SimpleStatement.builder("ALTER TABLE prepared_statement_invalidation_test ADD d int")
            .withConfigProfile(clusterRule.slowProfile())
            .build());
    BoundStatement bs = ps.bind(1);
    ResultSet rows = session.execute(bs);

    // Then
    ByteBuffer idAfter = ps.getResultMetadataId();
    assertThat(Bytes.toHexString(idAfter)).isNotEqualTo(Bytes.toHexString(idBefore));
    for (ColumnDefinitions columnDefinitions :
        ImmutableList.of(
            ps.getResultSetDefinitions(),
            bs.getPreparedStatement().getResultSetDefinitions(),
            rows.getColumnDefinitions())) {
      assertThat(columnDefinitions).hasSize(4);
      assertThat(columnDefinitions.get("d").getType()).isEqualTo(DataTypes.INT);
    }
  }

  @Test
  @CassandraRequirement(min = "4.0")
  public void should_update_metadata_when_schema_changed_across_pages() {
    // Given
    CqlSession session = clusterRule.session();
    PreparedStatement ps = session.prepare("SELECT * FROM prepared_statement_invalidation_test");
    ByteBuffer idBefore = ps.getResultMetadataId();
    assertThat(ps.getResultSetDefinitions()).hasSize(3);

    ResultSet rows = session.execute(ps.bind());
    assertThat(rows.isFullyFetched()).isFalse();
    assertThat(rows.getColumnDefinitions()).hasSize(3);
    assertThat(rows.getColumnDefinitions().contains("d")).isFalse();
    // Consume the first page
    int remaining = rows.getAvailableWithoutFetching();
    while (remaining-- > 0) {
      try {
        rows.one().getInt("d");
        fail("expected an error");
      } catch (ArrayIndexOutOfBoundsException e) {
        /*expected*/
      }
    }

    // When
    session.execute(
        SimpleStatement.builder("ALTER TABLE prepared_statement_invalidation_test ADD d int")
            .withConfigProfile(clusterRule.slowProfile())
            .build());

    // Then
    // this should trigger a background fetch of the second page, and therefore update the definitions
    for (Row row : rows) {
      assertThat(row.isNull("d")).isTrue();
    }
    assertThat(rows.getColumnDefinitions()).hasSize(4);
    assertThat(rows.getColumnDefinitions().get("d").getType()).isEqualTo(DataTypes.INT);
    // Should have updated the prepared statement too
    ByteBuffer idAfter = ps.getResultMetadataId();
    assertThat(Bytes.toHexString(idAfter)).isNotEqualTo(Bytes.toHexString(idBefore));
    assertThat(ps.getResultSetDefinitions()).hasSize(4);
    assertThat(ps.getResultSetDefinitions().get("d").getType()).isEqualTo(DataTypes.INT);
  }

  @Test
  @CassandraRequirement(min = "4.0")
  public void should_update_metadata_when_schema_changed_across_sessions() {
    // Given
    CqlSession session1 = clusterRule.session();
    CqlSession session2 = clusterRule.cluster().connect(clusterRule.keyspace());

    PreparedStatement ps1 =
        session1.prepare("SELECT * FROM prepared_statement_invalidation_test WHERE a = ?");
    PreparedStatement ps2 =
        session2.prepare("SELECT * FROM prepared_statement_invalidation_test WHERE a = ?");

    ByteBuffer id1a = ps1.getResultMetadataId();
    ByteBuffer id2a = ps2.getResultMetadataId();

    ResultSet rows1 = session1.execute(ps1.bind(1));
    ResultSet rows2 = session2.execute(ps2.bind(1));

    assertThat(rows1.getColumnDefinitions()).hasSize(3);
    assertThat(rows1.getColumnDefinitions().contains("d")).isFalse();
    assertThat(rows2.getColumnDefinitions()).hasSize(3);
    assertThat(rows2.getColumnDefinitions().contains("d")).isFalse();

    // When
    session1.execute("ALTER TABLE prepared_statement_invalidation_test ADD d int");

    rows1 = session1.execute(ps1.bind(1));
    rows2 = session2.execute(ps2.bind(1));

    ByteBuffer id1b = ps1.getResultMetadataId();
    ByteBuffer id2b = ps2.getResultMetadataId();

    // Then
    assertThat(Bytes.toHexString(id1b)).isNotEqualTo(Bytes.toHexString(id1a));
    assertThat(Bytes.toHexString(id2b)).isNotEqualTo(Bytes.toHexString(id2a));

    assertThat(ps1.getResultSetDefinitions()).hasSize(4);
    assertThat(ps1.getResultSetDefinitions().contains("d")).isTrue();
    assertThat(ps2.getResultSetDefinitions()).hasSize(4);
    assertThat(ps2.getResultSetDefinitions().contains("d")).isTrue();

    assertThat(rows1.getColumnDefinitions()).hasSize(4);
    assertThat(rows1.getColumnDefinitions().contains("d")).isTrue();
    assertThat(rows2.getColumnDefinitions()).hasSize(4);
    assertThat(rows2.getColumnDefinitions().contains("d")).isTrue();

    session2.close();
  }

  @Test
  @CassandraRequirement(min = "4.0")
  public void should_fail_to_reprepare_if_query_becomes_invalid() {
    // Given
    CqlSession session = clusterRule.session();
    session.execute("ALTER TABLE prepared_statement_invalidation_test ADD d int");
    PreparedStatement ps =
        session.prepare("SELECT a, b, c, d FROM prepared_statement_invalidation_test WHERE a = ?");
    session.execute("ALTER TABLE prepared_statement_invalidation_test DROP d");

    thrown.expect(InvalidQueryException.class);
    thrown.expectMessage("Undefined column name d");

    // When
    session.execute(ps.bind());
  }

  @Test
  @CassandraRequirement(min = "4.0")
  public void should_not_store_metadata_for_conditional_updates() {
    should_not_store_metadata_for_conditional_updates(clusterRule.session());
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_not_store_metadata_for_conditional_updates_in_legacy_protocol() {
    try (Cluster<CqlSession> cluster =
        ClusterUtils.newCluster(ccmRule, "protocol.version = V4", "request.timeout = 30 seconds")) {
      should_not_store_metadata_for_conditional_updates(cluster.connect(clusterRule.keyspace()));
    }
  }

  private void should_not_store_metadata_for_conditional_updates(CqlSession session) {
    // Given
    PreparedStatement ps =
        session.prepare(
            "INSERT INTO prepared_statement_invalidation_test (a, b, c) VALUES (?, ?, ?) IF NOT EXISTS");

    // Never store metadata in the prepared statement for conditional updates, since the result set can change
    // depending on the outcome.
    assertThat(ps.getResultSetDefinitions()).hasSize(0);
    ByteBuffer idBefore = ps.getResultMetadataId();

    // When
    ResultSet rs = session.execute(ps.bind(5, 5, 5));

    // Then
    // Successful conditional update => only contains the [applied] column
    assertThat(rs.wasApplied()).isTrue();
    assertThat(rs.getColumnDefinitions()).hasSize(1);
    assertThat(rs.getColumnDefinitions().get("[applied]").getType()).isEqualTo(DataTypes.BOOLEAN);
    // However the prepared statement shouldn't have changed
    assertThat(ps.getResultSetDefinitions()).hasSize(0);
    assertThat(Bytes.toHexString(ps.getResultMetadataId())).isEqualTo(Bytes.toHexString(idBefore));

    // When
    rs = session.execute(ps.bind(5, 5, 5));

    // Then
    // Failed conditional update => regular metadata
    assertThat(rs.wasApplied()).isFalse();
    assertThat(rs.getColumnDefinitions()).hasSize(4);
    Row row = rs.one();
    assertThat(row.getBoolean("[applied]")).isFalse();
    assertThat(row.getInt("a")).isEqualTo(5);
    assertThat(row.getInt("b")).isEqualTo(5);
    assertThat(row.getInt("c")).isEqualTo(5);
    // The prepared statement still shouldn't have changed
    assertThat(ps.getResultSetDefinitions()).hasSize(0);
    assertThat(Bytes.toHexString(ps.getResultMetadataId())).isEqualTo(Bytes.toHexString(idBefore));

    // When
    session.execute("ALTER TABLE prepared_statement_invalidation_test ADD d int");
    rs = session.execute(ps.bind(5, 5, 5));

    // Then
    // Failed conditional update => regular metadata that should also contain the new column
    assertThat(rs.wasApplied()).isFalse();
    assertThat(rs.getColumnDefinitions()).hasSize(5);
    row = rs.one();
    assertThat(row.getBoolean("[applied]")).isFalse();
    assertThat(row.getInt("a")).isEqualTo(5);
    assertThat(row.getInt("b")).isEqualTo(5);
    assertThat(row.getInt("c")).isEqualTo(5);
    assertThat(row.isNull("d")).isTrue();
    assertThat(ps.getResultSetDefinitions()).hasSize(0);
    assertThat(Bytes.toHexString(ps.getResultMetadataId())).isEqualTo(Bytes.toHexString(idBefore));
  }
}
