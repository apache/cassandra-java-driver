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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class TableOptionsIT {
  private static final CqlIdentifier READ_REPAIR_KEY = CqlIdentifier.fromCql("read_repair");
  private static final CqlIdentifier ADDITIONAL_WRITE_POLICY_KEY =
      CqlIdentifier.fromCql("additional_write_policy");

  private static CcmRule ccmRule = CcmRule.getInstance();
  // disable debouncer to speed up test.
  private static SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  .withDuration(DefaultDriverOption.METADATA_SCHEMA_WINDOW, Duration.ofSeconds(0))
                  .build())
          .build();

  @Rule public TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @Test
  @CassandraRequirement(min = "4.0", description = "This test covers Cassandra 4+ features")
  public void should_handle_cassandra4_table_options() {
    CqlSession session = sessionRule.session();

    // A simple table with read_repair and additional_write_policy options
    session.execute(
        "CREATE TABLE foo(k int, a text, PRIMARY KEY(k)) "
            + "WITH read_repair='NONE' AND additional_write_policy='40p'");

    TableMetadata fooMetadata =
        session
            .getMetadata()
            .getKeyspace(sessionRule.keyspace())
            .orElseThrow(AssertionError::new)
            .getTable("foo")
            .orElseThrow(AssertionError::new);

    assertThat(fooMetadata.getOptions())
        .containsEntry(READ_REPAIR_KEY, "NONE")
        .containsEntry(ADDITIONAL_WRITE_POLICY_KEY, "40p");
  }
}
