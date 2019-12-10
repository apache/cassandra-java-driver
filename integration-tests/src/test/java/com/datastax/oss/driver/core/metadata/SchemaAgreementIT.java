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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.loadbalancing.SortingLoadBalancingPolicy;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;

public class SchemaAgreementIT {

  private static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withNodes(3).build();
  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  .withClass(
                      DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                      SortingLoadBalancingPolicy.class)
                  .withDuration(
                      DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT,
                      Duration.ofSeconds(3))
                  .build())
          .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @Rule public TestName name = new TestName();

  @Test
  public void should_succeed_when_all_nodes_agree() {
    ResultSet result = createTable();

    assertThat(result.getExecutionInfo().isSchemaInAgreement()).isTrue();
    assertThat(SESSION_RULE.session().checkSchemaAgreement()).isTrue();
  }

  @Test
  public void should_fail_on_timeout() {
    CCM_RULE.getCcmBridge().pause(2);
    try {
      // Can't possibly agree since one node is paused.
      ResultSet result = createTable();

      assertThat(result.getExecutionInfo().isSchemaInAgreement()).isFalse();
      assertThat(SESSION_RULE.session().checkSchemaAgreement()).isFalse();
    } finally {
      CCM_RULE.getCcmBridge().resume(2);
    }
  }

  @Test
  public void should_agree_when_up_nodes_agree() {
    CCM_RULE.getCcmBridge().stop(2);
    try {
      // Should agree since up hosts should agree.
      ResultSet result = createTable();

      assertThat(result.getExecutionInfo().isSchemaInAgreement()).isTrue();
      assertThat(SESSION_RULE.session().checkSchemaAgreement()).isTrue();
    } finally {
      CCM_RULE.getCcmBridge().start(2);
    }
  }

  @Test
  public void should_fail_if_timeout_is_zero() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .withDuration(
                DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, Duration.ofSeconds(0))
            .build();
    try (CqlSession session = SessionUtils.newSession(CCM_RULE, SESSION_RULE.keyspace(), loader)) {
      ResultSet result = createTable(session);

      // Should not agree because schema metadata is disabled
      assertThat(result.getExecutionInfo().isSchemaInAgreement()).isFalse();
      assertThat(session.checkSchemaAgreement()).isFalse();
    }
  }

  private ResultSet createTable() {
    return createTable(SESSION_RULE.session());
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
