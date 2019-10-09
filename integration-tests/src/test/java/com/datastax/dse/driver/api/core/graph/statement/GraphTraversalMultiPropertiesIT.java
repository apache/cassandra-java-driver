/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph.statement;

import static com.datastax.dse.driver.api.core.graph.DseGraph.g;
import static com.datastax.dse.driver.api.core.graph.FluentGraphStatement.newInstance;
import static com.datastax.dse.driver.api.core.graph.SampleGraphScripts.ALLOW_SCANS;
import static com.datastax.dse.driver.api.core.graph.SampleGraphScripts.MAKE_STRICT;
import static com.datastax.dse.driver.api.core.graph.TinkerGraphAssertions.assertThat;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.api.testinfra.session.DseSessionRuleBuilder;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import java.util.Iterator;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@DseRequirement(min = "5.0.3", description = "DSE 5.0.3 required for remote TinkerPop support")
public class GraphTraversalMultiPropertiesIT {

  private static CustomCcmRule ccmRule = CustomCcmRule.builder().withDseWorkloads("graph").build();

  private static SessionRule<DseSession> sessionRule =
      new DseSessionRuleBuilder(ccmRule).withCreateGraph().build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  /** Builds a simple schema that provides for a vertex with a multi-cardinality property. */
  public static final String multiProps =
      MAKE_STRICT
          + ALLOW_SCANS
          + "schema.propertyKey('multi_prop').Text().multiple().create()\n"
          + "schema.vertexLabel('multi_v').properties('multi_prop').create()\n";

  /**
   * Ensures that a traversal that yields a vertex with a property name that is present multiple
   * times that the properties are parsed and made accessible via {@link
   * Vertex#properties(String...)}.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_parse_multiple_cardinality_properties() {
    // given a schema that defines multiple cardinality properties.
    sessionRule.session().execute(ScriptGraphStatement.newInstance(multiProps));

    // when adding a vertex with a multiple cardinality property
    GraphResultSet result =
        sessionRule
            .session()
            .execute(
                newInstance(
                    g.addV("multi_v")
                        .property("multi_prop", "Hello")
                        .property("multi_prop", "Sweet")
                        .property("multi_prop", "World")));

    Vertex v = result.one().asVertex();
    assertThat(v).hasProperty("multi_prop");

    Iterator<VertexProperty<String>> multiProp = v.properties("multi_prop");
    assertThat(multiProp)
        .toIterable()
        .extractingResultOf("value")
        .containsExactly("Hello", "Sweet", "World");
  }
}
