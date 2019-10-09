/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.testinfra.session.DseSessionRuleBuilder;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@DseRequirement(min = "5.1")
public class MetadataIT {

  private static CcmRule ccmRule = CcmRule.getInstance();

  private static SessionRule<DseSession> sessionRule = new DseSessionRuleBuilder(ccmRule).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @Test
  public void should_expose_dse_node_properties() {
    Node node = sessionRule.session().getMetadata().getNodes().values().iterator().next();

    // Basic checks as we want something that will work with a large range of DSE versions:
    assertThat(node.getExtras())
        .containsKeys(
            DseNodeProperties.DSE_VERSION,
            DseNodeProperties.DSE_WORKLOADS,
            DseNodeProperties.SERVER_ID);
    assertThat(node.getExtras().get(DseNodeProperties.DSE_VERSION)).isInstanceOf(Version.class);
    assertThat(node.getExtras().get(DseNodeProperties.SERVER_ID)).isInstanceOf(String.class);
    assertThat(node.getExtras().get(DseNodeProperties.DSE_WORKLOADS)).isInstanceOf(Set.class);
  }
}
