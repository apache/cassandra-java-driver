/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph.remote;

import static com.datastax.dse.driver.api.core.graph.SampleGraphScripts.ALLOW_SCANS;
import static com.datastax.dse.driver.api.core.graph.SampleGraphScripts.MAKE_STRICT;
import static com.datastax.dse.driver.api.core.graph.TinkerGraphAssertions.assertThat;

import com.datastax.dse.driver.api.core.graph.DseGraph;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.api.testinfra.session.DseSessionRule;
import com.datastax.dse.driver.api.testinfra.session.DseSessionRuleBuilder;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

// INFO: meta props are going away in NGDG

@DseRequirement(min = "5.0.3", description = "DSE 5.0.3 required for remote TinkerPop support")
public class GraphTraversalMetaPropertiesRemoteIT {

  private static CustomCcmRule ccmRule = CustomCcmRule.builder().withDseWorkloads("graph").build();

  private static DseSessionRule sessionRule =
      new DseSessionRuleBuilder(ccmRule).withCreateGraph().build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  private final GraphTraversalSource g =
      DseGraph.g.withRemote(DseGraph.remoteConnectionBuilder(sessionRule.session()).build());

  /** Builds a simple schema that provides for a vertex with a property with sub properties. */
  public static String metaProps =
      MAKE_STRICT
          + ALLOW_SCANS
          + "schema.propertyKey('sub_prop').Text().create()\n"
          + "schema.propertyKey('sub_prop2').Text().create()\n"
          + "schema.propertyKey('meta_prop').Text().properties('sub_prop', 'sub_prop2').create()\n"
          + "schema.vertexLabel('meta_v').properties('meta_prop').create()";

  /**
   * Ensures that a traversal that yields a vertex with a property that has its own properties that
   * is appropriately parsed and made accessible via {@link VertexProperty#property}.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_parse_meta_properties() {
    // given a schema that defines meta properties.
    sessionRule.session().execute(ScriptGraphStatement.newInstance(metaProps));

    // when adding a vertex with that meta property
    Vertex v =
        g.addV("meta_v")
            .property("meta_prop", "hello", "sub_prop", "hi", "sub_prop2", "hi2")
            .next();

    // then the created vertex should have the meta prop present with its sub properties.
    assertThat(v).hasProperty("meta_prop");
    VertexProperty<String> metaProp = v.property("meta_prop");
    assertThat(metaProp)
        .hasValue("hello")
        .hasProperty("sub_prop", "hi")
        .hasProperty("sub_prop2", "hi2");
  }
}
