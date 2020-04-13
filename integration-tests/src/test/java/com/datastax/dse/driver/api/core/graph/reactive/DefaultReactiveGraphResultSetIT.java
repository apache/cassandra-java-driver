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
package com.datastax.dse.driver.api.core.graph.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.internal.core.graph.GraphProtocol;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import io.reactivex.Flowable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

@DseRequirement(min = "6.8.0", description = "Graph paging requires DSE 6.8+")
@RunWith(DataProviderRunner.class)
public class DefaultReactiveGraphResultSetIT {

  private static CustomCcmRule ccmRule = CustomCcmRule.builder().withDseWorkloads("graph").build();

  private static SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule)
          .withCreateGraph()
          .withCoreEngine()
          .withGraphProtocol(GraphProtocol.GRAPH_BINARY_1_0.toInternalCode())
          .build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @BeforeClass
  public static void setupSchema() {
    sessionRule
        .session()
        .execute(
            ScriptGraphStatement.newInstance(
                    "schema.vertexLabel('person')"
                        + ".partitionBy('pk', Int)"
                        + ".clusterBy('cc', Int)"
                        + ".property('name', Text)"
                        + ".create();")
                .setGraphName(sessionRule.getGraphName()));
    for (int i = 1; i <= 1000; i++) {
      sessionRule
          .session()
          .execute(
              ScriptGraphStatement.newInstance(
                      String.format(
                          "g.addV('person').property('pk',0).property('cc',%d).property('name', '%s');",
                          i, "user" + i))
                  .setGraphName(sessionRule.getGraphName()));
    }
  }

  @Test
  @DataProvider(
      value = {"1", "10", "100", "999", "1000", "1001", "2000"},
      format = "%m [page size %p[0]]")
  public void should_retrieve_all_rows(int pageSize) {
    DriverExecutionProfile profile =
        sessionRule
            .session()
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withInt(DseDriverOption.GRAPH_CONTINUOUS_PAGING_PAGE_SIZE, pageSize);
    ScriptGraphStatement statement =
        ScriptGraphStatement.builder("g.V()").setExecutionProfile(profile).build();
    ReactiveGraphResultSet rs = sessionRule.session().executeReactive(statement);
    List<ReactiveGraphNode> results = Flowable.fromPublisher(rs).toList().blockingGet();
    assertThat(results.size()).isEqualTo(1000);
    Set<ExecutionInfo> expectedExecInfos = new LinkedHashSet<>();
    for (ReactiveGraphNode row : results) {
      assertThat(row.getExecutionInfo()).isNotNull();
      assertThat(row.isVertex()).isTrue();
      expectedExecInfos.add(row.getExecutionInfo());
    }
    List<ExecutionInfo> execInfos =
        Flowable.<ExecutionInfo>fromPublisher(rs.getExecutionInfos()).toList().blockingGet();
    // DSE may send an empty page as it can't always know if it's done paging or not yet.
    // See: CASSANDRA-8871. In this case, this page's execution info appears in
    // rs.getExecutionInfos(), but is not present in expectedExecInfos since the page did not
    // contain any rows.
    assertThat(execInfos).containsAll(expectedExecInfos);
  }
}
