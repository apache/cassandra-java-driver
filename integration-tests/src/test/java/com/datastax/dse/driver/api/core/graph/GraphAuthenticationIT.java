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

import static com.datastax.dse.driver.api.core.graph.TinkerGraphAssertions.assertThat;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

@BackendRequirement(
    type = BackendType.DSE,
    minInclusive = "5.0.0",
    description = "DSE 5 required for Graph")
public class GraphAuthenticationIT {

  @ClassRule
  public static CustomCcmRule ccm =
      CustomCcmRule.builder()
          .withDseConfiguration("authentication_options.enabled", true)
          .withJvmArgs("-Dcassandra.superuser_setup_delay_ms=0")
          .withDseWorkloads("graph")
          .build();

  @BeforeClass
  public static void sleepForAuth() {
    if (ccm.getCassandraVersion().compareTo(Version.V2_2_0) < 0) {
      // Sleep for 1 second to allow C* auth to do its work.  This is only needed for 2.1
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

  @Test
  public void should_execute_graph_query_on_authenticated_connection() {
    CqlSession dseSession =
        SessionUtils.newSession(
            ccm,
            DriverConfigLoader.programmaticBuilder()
                .withString(DseDriverOption.AUTH_PROVIDER_AUTHORIZATION_ID, "")
                .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "cassandra")
                .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "cassandra")
                .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class)
                .build());

    GraphNode gn =
        dseSession.execute(ScriptGraphStatement.newInstance("1+1").setSystemQuery(true)).one();
    assertThat(gn).isNotNull();
    assertThat(gn.asInt()).isEqualTo(2);
  }
}
