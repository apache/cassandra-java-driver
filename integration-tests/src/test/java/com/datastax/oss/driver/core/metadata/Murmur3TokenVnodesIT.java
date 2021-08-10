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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import java.time.Duration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@CassandraRequirement(
    max = "4.0-beta4",
    // TODO Re-enable when CASSANDRA-16364 is fixed
    description = "TODO Re-enable when CASSANDRA-16364 is fixed")
public class Murmur3TokenVnodesIT extends TokenITBase {

  private static final CustomCcmRule CCM_RULE =
      CustomCcmRule.builder()
          .withNodes(3)
          .withCreateOption("--vnodes")
          .withCassandraConfiguration("range_request_timeout_in_ms", 45_000)
          .withCassandraConfiguration("read_request_timeout_in_ms", 45_000)
          .withCassandraConfiguration("write_request_timeout_in_ms", 45_000)
          .withCassandraConfiguration("request_timeout_in_ms", 45_000)
          .build();

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE)
          .withKeyspace(false)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  public Murmur3TokenVnodesIT() {
    super("org.apache.cassandra.dht.Murmur3Partitioner", Murmur3Token.class, true);
  }

  @Override
  protected CqlSession session() {
    return SESSION_RULE.session();
  }

  @BeforeClass
  public static void createSchema() {
    createSchema(SESSION_RULE.session());
  }
}
