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
package com.datastax.oss.driver.core.compression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.IsolatedTests;
import java.time.Duration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(IsolatedTests.class)
public class HeapCompressionIT {

  static {
    System.setProperty("io.netty.noPreferDirect", "true");
    System.setProperty("io.netty.noUnsafe", "true");
  }

  private static CustomCcmRule ccmRule = CustomCcmRule.builder().build();

  private static SessionRule<CqlSession> schemaSessionRule =
      SessionRule.builder(ccmRule)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  .build())
          .build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(schemaSessionRule);

  @BeforeClass
  public static void setup() {
    schemaSessionRule
        .session()
        .execute("CREATE TABLE test (k text PRIMARY KEY, t text, i int, f float)");
  }

  /**
   * Validates that Snappy compression still works when using heap buffers.
   *
   * @test_category connection:compression
   * @expected_result session established and queries made successfully using it.
   */
  @Test
  public void should_execute_queries_with_snappy_compression() throws Exception {
    createAndCheckCluster("snappy");
  }

  /**
   * Validates that LZ4 compression still works when using heap buffers.
   *
   * @test_category connection:compression
   * @expected_result session established and queries made successfully using it.
   */
  @Test
  public void should_execute_queries_with_lz4_compression() throws Exception {
    createAndCheckCluster("lz4");
  }

  private void createAndCheckCluster(String compressorOption) {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, compressorOption)
            .build();
    try (CqlSession session =
        SessionUtils.newSession(ccmRule, schemaSessionRule.keyspace(), loader)) {
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

      ExecutionInfo executionInfo = rs.getExecutionInfo();
      // There's not much more we can check without hard-coding sizes.
      // We are testing with small responses, so the compressed payload is not even guaranteed to be
      // smaller.
      assertThat(executionInfo.getResponseSizeInBytes()).isGreaterThan(0);
      assertThat(executionInfo.getCompressedResponseSizeInBytes()).isGreaterThan(0);
    }
  }
}
