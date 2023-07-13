/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.dse.driver.api.core.graph;

import static com.datastax.dse.driver.api.core.graph.ScriptGraphStatement.newInstance;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import java.time.Duration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@DseRequirement(min = "5.0.0", description = "DSE 5 required for Graph")
public class GraphTimeoutsIT {

  public static CustomCcmRule ccmRule = CustomCcmRule.builder().withDseWorkloads("graph").build();

  public static SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule).withCreateGraph().build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @Test
  public void should_have_driver_wait_indefinitely_by_default_and_server_return_timeout_response() {
    long desiredTimeout = 2500L;

    DriverExecutionProfile drivertest1 =
        sessionRule
            .session()
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withString(DseDriverOption.GRAPH_TRAVERSAL_SOURCE, "drivertest1");

    // We could have done with the server's default but it's 30 secs so the test would have taken at
    // least
    // that time. So we simulate a server timeout change.
    sessionRule
        .session()
        .execute(
            newInstance(
                    "graph.schema().config().option(\"graph.traversal_sources.drivertest1.evaluation_timeout\").set('"
                        + desiredTimeout
                        + " ms')")
                .setExecutionProfile(drivertest1));

    try {
      // The driver should wait indefinitely, but the server should timeout first.
      sessionRule
          .session()
          .execute(
              newInstance("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(35000L);1+1")
                  .setExecutionProfile(drivertest1));
      fail("The request should have timed out");
    } catch (InvalidQueryException e) {
      assertThat(e.toString())
          .contains("evaluation exceeded", "threshold of ", desiredTimeout + " ms");
    }
  }

  @Test
  public void should_not_take_into_account_request_timeout_if_more_than_server_timeout() {
    long desiredTimeout = 1000L;
    int clientTimeout = 32000;

    DriverExecutionProfile drivertest2 =
        sessionRule
            .session()
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withString(DseDriverOption.GRAPH_TRAVERSAL_SOURCE, "drivertest2")
            .withDuration(DseDriverOption.GRAPH_TIMEOUT, Duration.ofMillis(clientTimeout));

    sessionRule
        .session()
        .execute(
            newInstance(
                    "graph.schema().config().option(\"graph.traversal_sources.drivertest2.evaluation_timeout\").set('"
                        + desiredTimeout
                        + " ms')")
                .setExecutionProfile(drivertest2));

    try {
      // The driver should wait 32 secs, but the server should timeout first.
      sessionRule
          .session()
          .execute(
              newInstance("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(35000L);1+1")
                  .setExecutionProfile(drivertest2));
      fail("The request should have timed out");
    } catch (InvalidQueryException e) {
      assertThat(e.toString())
          .contains("evaluation exceeded", "threshold of ", Long.toString(desiredTimeout), "ms");
    }
  }

  @Test
  public void should_take_into_account_request_timeout_if_less_than_server_timeout() {
    long serverTimeout = 10000L;
    int desiredTimeout = 1000;

    DriverExecutionProfile drivertest3 =
        sessionRule
            .session()
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withString(DseDriverOption.GRAPH_TRAVERSAL_SOURCE, "drivertest3");

    // We could have done with the server's default but it's 30 secs so the test would have taken at
    // least
    // that time. Also, we don't want to rely on server's default. So we simulate a server timeout
    // change.
    sessionRule
        .session()
        .execute(
            ScriptGraphStatement.newInstance(
                    "graph.schema().config().option(\"graph.traversal_sources.drivertest3.evaluation_timeout\").set('"
                        + serverTimeout
                        + " ms')")
                .setExecutionProfile(drivertest3));

    try {
      // The timeout on the request is lower than what's defined server side, so it should be taken
      // into account.
      sessionRule
          .session()
          .execute(
              ScriptGraphStatement.newInstance(
                      "java.util.concurrent.TimeUnit.MILLISECONDS.sleep(35000L);1+1")
                  .setExecutionProfile(
                      drivertest3.withDuration(
                          DseDriverOption.GRAPH_TIMEOUT, Duration.ofMillis(desiredTimeout))));
      fail("The request should have timed out");
    } catch (Exception e) {
      // Since server timeout == client timeout, locally concurrency is likely to happen.
      // We cannot know for sure if it will be a Client timeout error, or a Server timeout, and
      // during tests, both happened and not deterministically.
      if (e instanceof InvalidQueryException) {
        assertThat(e.toString())
            .contains("evaluation exceeded", "threshold of ", desiredTimeout + " ms");
      } else {
        assertThat(e).isInstanceOf(DriverTimeoutException.class);
      }
    }
  }
}
