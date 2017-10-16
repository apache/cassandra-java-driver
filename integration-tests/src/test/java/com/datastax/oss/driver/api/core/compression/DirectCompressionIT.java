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
package com.datastax.oss.driver.api.core.compression;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.session.CqlSession;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

public class DirectCompressionIT {

  @ClassRule public static CcmRule ccmRule = CcmRule.getInstance();

  @ClassRule
  public static ClusterRule schemaClusterRule =
      new ClusterRule(ccmRule, "request.timeout = 30 seconds");

  @BeforeClass
  public static void setup() {
    schemaClusterRule
        .session()
        .execute("CREATE TABLE test (k text PRIMARY KEY, t text, i int, f float)");
  }

  /**
   * Validates that a cluster configured with Snappy compression and can execute queries that insert
   * and retrieve data.
   *
   * @test_category connection:compression
   * @expected_result session established and queries made successfully using it.
   */
  @Test
  public void should_execute_queries_with_snappy_compression() throws Exception {
    createAndCheckCluster(
        "protocol.compressor.class = "
            + "com.datastax.oss.driver.internal.core.protocol.SnappyCompressor");
  }

  /**
   * Validates that a cluster configured with LZ4 compression and can execute queries that insert
   * and retrieve data.
   *
   * @test_category connection:compression
   * @expected_result session established and queries made successfully using it.
   */
  @Test
  public void should_execute_queries_with_lz4_compression() throws Exception {
    createAndCheckCluster(
        "protocol.compressor.class = "
            + "com.datastax.oss.driver.internal.core.protocol.Lz4Compressor");
  }

  private void createAndCheckCluster(String compressorOption) {

    try (Cluster<CqlSession> cluster = ClusterUtils.newCluster(ccmRule, compressorOption)) {
      CqlSession session = cluster.connect(schemaClusterRule.keyspace());

      // Run a couple of simple test queries
      ResultSet rs =
          session.execute(
              SimpleStatement.newInstance(
                  "INSERT INTO test (k, t, i, f) VALUES (?, ?, ?, ?)", "key", "foo", 42, 24.03f));
      assertThat(rs.iterator().hasNext()).isFalse();

      ResultSet rs1 = session.execute("SELECT * FROM test WHERE k = 'key'");
      assertThat(rs1.iterator().hasNext()).isTrue();
      Row row = rs1.iterator().next();
      assertThat(rs1.iterator().hasNext()).isFalse();
      assertThat(row.getString("k")).isEqualTo("key");
      assertThat(row.getString("t")).isEqualTo("foo");
      assertThat(row.getInt("i")).isEqualTo(42);
      assertThat(row.getFloat("f")).isEqualTo(24.03f, offset(0.1f));
    }
  }
}
