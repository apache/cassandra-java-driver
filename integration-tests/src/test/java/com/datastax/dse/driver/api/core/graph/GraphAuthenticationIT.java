/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph;

import static com.datastax.dse.driver.api.core.graph.TinkerGraphAssertions.assertThat;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.config.DseDriverConfigLoader;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.internal.core.auth.DsePlainTextAuthProvider;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

@DseRequirement(min = "5.0.0", description = "DSE 5 required for Graph")
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
    DseSession dseSession =
        SessionUtils.newSession(
            ccm,
            DseDriverConfigLoader.programmaticBuilder()
                .withString(DseDriverOption.AUTH_PROVIDER_AUTHORIZATION_ID, "")
                .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "cassandra")
                .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "cassandra")
                .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, DsePlainTextAuthProvider.class)
                .build());

    GraphNode gn =
        dseSession.execute(ScriptGraphStatement.newInstance("1+1").setSystemQuery(true)).one();
    assertThat(gn).isNotNull();
    assertThat(gn.asInt()).isEqualTo(2);
  }
}
