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
package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@CassandraVersion("4.0")
public class NowInSecondsTest extends CCMTestsSupport {

  private static final String WRITE_QUERY = "INSERT INTO test (k,v) VALUES (1,1) USING TTL 20";
  private static final Statement READ_STATEMENT =
      new SimpleStatement("SELECT TTL(v) FROM test WHERE k = 1");

  @Override
  public Cluster.Builder createClusterBuilder() {
    return super.createClusterBuilder().allowBetaProtocolVersion();
  }

  @BeforeMethod(groups = "short")
  public void setup() {
    execute("DROP TABLE IF EXISTS test", "CREATE TABLE test(k int PRIMARY KEY, v int)");
  }

  @Test(groups = "short")
  public void should_use_now_in_seconds_with_simple_statement() {
    should_use_now_in_seconds(new SimpleStatement(WRITE_QUERY));
  }

  @Test(groups = "short")
  public void should_use_now_in_seconds_with_bound_statement() {
    PreparedStatement preparedStatement = session().prepare(WRITE_QUERY);
    should_use_now_in_seconds(preparedStatement.bind());
  }

  @Test(groups = "short")
  public void should_use_now_in_seconds_with_batch_statement() {
    should_use_now_in_seconds(
        new BatchStatement(BatchStatement.Type.LOGGED).add(new SimpleStatement(WRITE_QUERY)));
  }

  private void should_use_now_in_seconds(Statement writeStatement) {
    // When
    // insert at t = 0 with TTL 20
    session().execute(writeStatement.setNowInSeconds(0));
    // read TTL at t = 10
    ResultSet rs = session().execute(READ_STATEMENT.setNowInSeconds(10));
    int remainingTtl = rs.one().getInt(0);

    // Then
    assertThat(remainingTtl).isEqualTo(10);
  }
}
