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
package com.datastax.oss.driver.core.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.function.Function;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@CassandraRequirement(min = "4.0")
@DseRequirement(
    // Use next version -- not sure if it will be in by then, but as a reminder to check
    min = "7.0",
    description = "Feature not available in DSE yet")
public class NowInSecondsIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @Before
  public void setup() {
    for (String statement :
        ImmutableList.of(
            "DROP TABLE IF EXISTS test", "CREATE TABLE test(k int PRIMARY KEY, v int)")) {
      SESSION_RULE
          .session()
          .execute(
              SimpleStatement.newInstance(statement)
                  .setExecutionProfile(SESSION_RULE.slowProfile()));
    }
  }

  @Test
  public void should_use_now_in_seconds_with_simple_statement() {
    should_use_now_in_seconds(SimpleStatement::newInstance);
  }

  @Test
  public void should_use_now_in_seconds_with_bound_statement() {
    should_use_now_in_seconds(
        queryString -> {
          PreparedStatement preparedStatement = SESSION_RULE.session().prepare(queryString);
          return preparedStatement.bind();
        });
  }

  @Test
  public void should_use_now_in_seconds_with_batch_statement() {
    should_use_now_in_seconds(
        queryString ->
            BatchStatement.newInstance(BatchType.LOGGED, SimpleStatement.newInstance(queryString)));
  }

  private <StatementT extends Statement<StatementT>> void should_use_now_in_seconds(
      Function<String, StatementT> buildWriteStatement) {
    CqlSession session = SESSION_RULE.session();

    // Given
    StatementT writeStatement =
        buildWriteStatement.apply("INSERT INTO test (k,v) VALUES (1,1) USING TTL 20");
    SimpleStatement readStatement =
        SimpleStatement.newInstance("SELECT TTL(v) FROM test WHERE k = 1");

    // When
    // insert at t = 0 with TTL 20
    session.execute(writeStatement.setNowInSeconds(0));
    // read TTL at t = 10
    ResultSet rs = session.execute(readStatement.setNowInSeconds(10));
    int remainingTtl = rs.one().getInt(0);

    // Then
    assertThat(remainingTtl).isEqualTo(10);
  }
}
