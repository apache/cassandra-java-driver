/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph;

import static com.datastax.dse.driver.api.core.graph.predicates.CqlCollection.contains;
import static com.datastax.dse.driver.api.core.graph.predicates.CqlCollection.containsKey;
import static com.datastax.dse.driver.api.core.graph.predicates.CqlCollection.entryEq;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.graph.predicates.CqlCollection;
import com.datastax.dse.driver.api.testinfra.session.DseSessionRule;
import com.datastax.dse.driver.api.testinfra.session.DseSessionRuleBuilder;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@DseRequirement(min = "6.8", description = "DSE 6.8.0 required for collection predicates support")
public class CqlCollectionIT {

  private static final CustomCcmRule CCM_RULE =
      CustomCcmRule.builder().withDseWorkloads("graph").build();

  private static DseSessionRule SESSION_RULE =
      new DseSessionRuleBuilder(CCM_RULE)
          .withCreateGraph()
          .withCoreEngine()
          .withGraphProtocol("graph-binary-1.0")
          .build();

  @ClassRule public static TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private final GraphTraversalSource g =
      AnonymousTraversalSource.traversal()
          .withRemote(DseGraph.remoteConnectionBuilder(SESSION_RULE.session()).build());

  @BeforeClass
  public static void setup() {
    for (String setupQuery : createSchema()) {
      SESSION_RULE.session().execute(ScriptGraphStatement.newInstance(setupQuery));
    }
  }

  private static final Collection<String> createSchema() {
    return ImmutableList.of(
        "schema.vertexLabel('software').ifNotExists().partitionBy('name', Varchar)"
            + ".property('myList', listOf(Varchar))"
            + ".property('mySet', setOf(Varchar))"
            + ".property('myMapKeys', mapOf(Varchar, Int))"
            + ".property('myMapValues', mapOf(Int, Varchar))"
            + ".property('myMapEntries', mapOf(Int, Varchar))"
            + ".property('myFrozenList', frozen(listOf(Varchar)))"
            + ".property('myFrozenSet', frozen(setOf(Float)))"
            + ".property('myFrozenMap', frozen(mapOf(Int, Varchar)))"
            + ".create()",
        "schema.vertexLabel('software').secondaryIndex('by_myList').ifNotExists().by('myList').create();"
            + "schema.vertexLabel('software').secondaryIndex('by_mySet').ifNotExists().by('mySet').create();"
            + "schema.vertexLabel('software').secondaryIndex('by_myMapKeys').ifNotExists().by('myMapKeys').indexKeys().create();"
            + "schema.vertexLabel('software').secondaryIndex('by_myMapValues').ifNotExists().by('myMapValues').indexValues().create();"
            + "schema.vertexLabel('software').secondaryIndex('by_myMapEntries').ifNotExists().by('myMapEntries').indexEntries().create();"
            + "schema.vertexLabel('software').secondaryIndex('by_myFrozenList').ifNotExists().by('myFrozenList').indexFull().create();"
            + "schema.vertexLabel('software').secondaryIndex('by_myFrozenSet').ifNotExists().by('myFrozenSet').indexFull().create();"
            + "schema.vertexLabel('software').secondaryIndex('by_myFrozenMap').ifNotExists().by('myFrozenMap').indexFull().create()");
  }

  @Test
  public void should_apply_contains_predicate_to_non_frozen_list() {
    DseSession session = SESSION_RULE.session();

    List<String> myList1 = com.google.common.collect.ImmutableList.of("apple", "banana");
    List<String> myList2 = com.google.common.collect.ImmutableList.of("cranberry", "orange");

    session.execute(
        FluentGraphStatement.newInstance(
            DseGraph.g
                .addV("software")
                .property("name", "dse list 1")
                .property("myList", myList1)));
    session.execute(
        FluentGraphStatement.newInstance(
            DseGraph.g
                .addV("software")
                .property("name", "dse list 2")
                .property("myList", myList2)));

    assertThat(g.V().has("software", "myList", contains("apple")).values("myList").toList())
        .hasSize(1)
        .contains(myList1)
        .doesNotContain(myList2);
    assertThat(g.V().has("software", "myList", contains("strawberry")).toList()).isEmpty();
  }

  @Test
  public void should_apply_contains_predicate_to_non_frozen_set() {
    DseSession session = SESSION_RULE.session();

    Set<String> mySet1 = ImmutableSet.of("apple", "banana");
    Set<String> mySet2 = ImmutableSet.of("cranberry", "orange");

    session.execute(
        FluentGraphStatement.newInstance(
            DseGraph.g.addV("software").property("name", "dse set 1").property("mySet", mySet1)));
    session.execute(
        FluentGraphStatement.newInstance(
            DseGraph.g.addV("software").property("name", "dse set 2").property("mySet", mySet2)));

    assertThat(g.V().has("software", "mySet", contains("apple")).values("mySet").toList())
        .hasSize(1)
        .contains(mySet1)
        .doesNotContain(mySet2);
    assertThat(g.V().has("software", "mySet", contains("strawberry")).toList()).isEmpty();
  }

  @Test
  public void should_apply_containsKey_predicate_to_non_frozen_map() {
    DseSession session = SESSION_RULE.session();

    Map<String, Integer> myMap1 = ImmutableMap.<String, Integer>builder().put("id1", 1).build();
    Map<String, Integer> myMap2 = ImmutableMap.<String, Integer>builder().put("id2", 2).build();

    session.execute(
        FluentGraphStatement.newInstance(
            DseGraph.g
                .addV("software")
                .property("name", "dse map containsKey 1")
                .property("myMapKeys", myMap1)));
    session.execute(
        FluentGraphStatement.newInstance(
            DseGraph.g
                .addV("software")
                .property("name", "dse map containsKey 2")
                .property("myMapKeys", myMap2)));

    assertThat(g.V().has("software", "myMapKeys", containsKey("id1")).values("myMapKeys").toList())
        .hasSize(1)
        .contains(myMap1)
        .doesNotContain(myMap2);
    assertThat(g.V().has("software", "myMapKeys", containsKey("id3")).toList()).isEmpty();
  }

  @Test
  public void should_apply_containsValue_predicate_to_non_frozen_map() {
    DseSession session = SESSION_RULE.session();

    Map<Integer, String> myMap1 = ImmutableMap.<Integer, String>builder().put(11, "abc").build();
    Map<Integer, String> myMap2 = ImmutableMap.<Integer, String>builder().put(22, "def").build();

    session.execute(
        FluentGraphStatement.newInstance(
            DseGraph.g
                .addV("software")
                .property("name", "dse map containsValue 1")
                .property("myMapValues", myMap1)));
    session.execute(
        FluentGraphStatement.newInstance(
            DseGraph.g
                .addV("software")
                .property("name", "dse map containsValue 2")
                .property("myMapValues", myMap2)));
    assertThat(
            g.V()
                .has("software", "myMapValues", CqlCollection.containsValue("abc"))
                .values("myMapValues")
                .toList())
        .hasSize(1)
        .contains(myMap1)
        .doesNotContain(myMap2);
    assertThat(g.V().has("software", "myMapValues", CqlCollection.containsValue("xyz")).toList())
        .isEmpty();
  }

  @Test
  public void should_apply_entryEq_predicate_to_non_frozen_map() {
    DseSession session = SESSION_RULE.session();

    Map<Integer, String> myMap1 = ImmutableMap.<Integer, String>builder().put(11, "abc").build();
    Map<Integer, String> myMap2 = ImmutableMap.<Integer, String>builder().put(22, "def").build();

    session.execute(
        FluentGraphStatement.newInstance(
            DseGraph.g
                .addV("software")
                .property("name", "dse map entryEq 1")
                .property("myMapEntries", myMap1)));
    session.execute(
        FluentGraphStatement.newInstance(
            DseGraph.g
                .addV("software")
                .property("name", "dse map entryEq 2")
                .property("myMapEntries", myMap2)));
    assertThat(
            g.V()
                .has("software", "myMapEntries", entryEq(11, "abc"))
                .values("myMapEntries")
                .toList())
        .hasSize(1)
        .contains(myMap1)
        .doesNotContain(myMap2);
    assertThat(g.V().has("software", "myMapEntries", entryEq(11, "xyz")).toList()).isEmpty();
    assertThat(g.V().has("software", "myMapEntries", entryEq(33, "abc")).toList()).isEmpty();
    assertThat(g.V().has("software", "myMapEntries", entryEq(33, "xyz")).toList()).isEmpty();
  }
}
