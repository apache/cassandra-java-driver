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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.graph.predicates.Search;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

@DseRequirement(min = "5.1", description = "DSE 5.1 required for graph geo indexing")
@RunWith(DataProviderRunner.class)
public class GraphTextSearchIndexIT {

  private static CustomCcmRule ccmRule =
      CustomCcmRule.builder().withDseWorkloads("graph", "solr").build();

  private static SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule).withCreateGraph().build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  private final GraphTraversalSource g =
      DseGraph.g.withRemote(DseGraph.remoteConnectionBuilder(sessionRule.session()).build());

  /**
   * A schema representing an address book with 3 properties (full_name_*, description_*, alias_*)
   * created for each type of index (search, secondary, materialized).
   */
  public static Collection<String> textIndices() {
    Object[][] providerIndexTypes = indexTypes();
    String[] indexTypes = new String[providerIndexTypes.length];
    for (int i = 0; i < providerIndexTypes.length; i++) {
      indexTypes[i] = (String) providerIndexTypes[i][0];
    }

    StringBuilder schema = new StringBuilder("");
    StringBuilder propertyKeys = new StringBuilder("");
    StringBuilder vertexLabel = new StringBuilder("schema.vertexLabel('user').properties(");
    StringBuilder indices = new StringBuilder("");
    StringBuilder vertex0 = new StringBuilder("g.addV('user')");
    StringBuilder vertex1 = new StringBuilder("g.addV('user')");
    StringBuilder vertex2 = new StringBuilder("g.addV('user')");
    StringBuilder vertex3 = new StringBuilder("g.addV('user')");

    ArrayList<String> propertyNames = new ArrayList<String>();
    for (String indexType : indexTypes) {
      propertyKeys.append(
          String.format(
              "schema.propertyKey('full_name_%s').Text().create()\n"
                  + "schema.propertyKey('description_%s').Text().create()\n"
                  + "schema.propertyKey('alias_%s').Text().create()\n",
              indexType, indexType, indexType));

      propertyNames.add("'full_name_" + indexType + "'");
      propertyNames.add("'description_" + indexType + "'");
      propertyNames.add("'alias_" + indexType + "'");

      if (indexType.equals("search")) {
        indices.append(
            "schema.vertexLabel('user').index('search').search().by('full_name_search').asString().by('description_search').asText().by('alias_search').asString().add()\n");
      } else {
        indices.append(
            String.format(
                "schema.vertexLabel('user').index('by_full_name_%s').%s().by('full_name_%s').add()\n",
                indexType, indexType, indexType));
        indices.append(
            String.format(
                "schema.vertexLabel('user').index('by_description_%s').%s().by('description_%s').add()\n",
                indexType, indexType, indexType));
        indices.append(
            String.format(
                "schema.vertexLabel('user').index('by_alias_name_%s').%s().by('alias_%s').add()\n",
                indexType, indexType, indexType));
      }

      vertex0.append(
          String.format(
              ".property('full_name_%s', 'Paul Thomas Joe').property('description_%s', 'Lives by the hospital').property('alias_%s', 'mario')",
              indexType, indexType, indexType));
      vertex1.append(
          String.format(
              ".property('full_name_%s', 'George Bill Steve').property('description_%s', 'A cold dude').property('alias_%s', 'wario')",
              indexType, indexType, indexType));
      vertex2.append(
          String.format(
              ".property('full_name_%s', 'James Paul Joe').property('description_%s', 'Likes to hang out').property('alias_%s', 'bowser')",
              indexType, indexType, indexType));
      vertex3.append(
          String.format(
              ".property('full_name_%s', 'Jill Alice').property('description_%s', 'Enjoys a very nice cold coca cola').property('alias_%s', 'peach')",
              indexType, indexType, indexType));
    }

    vertexLabel.append(Joiner.on(", ").join(propertyNames));
    vertexLabel.append(").create()\n");

    schema.append(propertyKeys).append(vertexLabel).append(indices);

    return Lists.newArrayList(
        SampleGraphScripts.MAKE_STRICT,
        schema.toString(),
        vertex0.toString(),
        vertex1.toString(),
        vertex2.toString(),
        vertex3.toString());
  }

  @BeforeClass
  public static void setup() {
    for (String setupQuery : textIndices()) {
      sessionRule.session().execute(ScriptGraphStatement.newInstance(setupQuery));
    }

    ccmRule.getCcmBridge().reloadCore(1, sessionRule.getGraphName(), "user_p", true);
  }

  @DataProvider
  public static Object[][] indexTypes() {
    return new Object[][] {{"search"}

      // for some reason, materialized and secondary indices have decided not to work
      // I get an exception saying "there is no index for this query, here is the defined
      // indices: " and the list contains the indices that are needed. Mysterious.
      // There may be something to do with differences in the CCMBridge adapter of the new
      // driver, some changes make materialized views and secondary indices to be not
      // considered for graph:
      //
      //  , {"materialized"}
      //  , {"secondary"}
    };
  }

  /**
   * Validates that a graph traversal can be made by using a Search prefix predicate on an indexed
   * property of the given type.
   *
   * <p>Finds all 'user' vertices having a 'full_name' property beginning with 'Paul'.
   *
   * @test_category dse:graph
   */
  @UseDataProvider("indexTypes")
  @Test
  public void search_by_prefix_search(String indexType) {
    // Only one user with full_name starting with Paul.
    GraphTraversal<Vertex, String> traversal =
        g.V()
            .has("user", "full_name_" + indexType, Search.prefix("Paul"))
            .values("full_name_" + indexType);
    assertThat(traversal.toList()).containsOnly("Paul Thomas Joe");
  }

  /**
   * Validates that a graph traversal can be made by using a Search regex predicate on an indexed
   * property of the given type.
   *
   * <p>Finds all 'user' vertices having a 'full_name' property matching regex '.*Paul.*'.
   *
   * @test_category dse:graph
   */
  @UseDataProvider("indexTypes")
  @Test
  public void search_by_regex(String indexType) {
    // Only two people with names containing pattern for Paul.
    GraphTraversal<Vertex, String> traversal =
        g.V()
            .has("user", "full_name_" + indexType, Search.regex(".*Paul.*"))
            .values("full_name_" + indexType);
    assertThat(traversal.toList()).containsOnly("Paul Thomas Joe", "James Paul Joe");
  }

  /**
   * Validates that a graph traversal can be made by using a Search fuzzy predicate on an indexed
   * property of the given type.
   *
   * <p>Finds all 'user' vertices having a 'alias' property matching 'awrio' with a fuzzy distance
   * of 1.
   *
   * @test_category dse:graph
   */
  @UseDataProvider("indexTypes")
  @Test
  @DseRequirement(min = "5.1.0")
  public void search_by_fuzzy(String indexType) {
    // Alias matches 'awrio' fuzzy
    GraphTraversal<Vertex, String> traversal =
        g.V()
            .has("user", "alias_" + indexType, Search.fuzzy("awrio", 1))
            .values("full_name_" + indexType);
    // Should not match 'Paul Thomas Joe' since alias is 'mario', which is at distance 2 of 'awrio'
    // (a -> m, w -> a)
    // Should match 'George Bill Steve' since alias is 'wario' witch matches 'awrio' within a
    // distance of 1 (transpose w with a).
    assertThat(traversal.toList()).containsOnly("George Bill Steve");
  }

  /**
   * Validates that a graph traversal can be made by using a Search token predicate on an indexed
   * property of the given type.
   *
   * <p>Finds all 'user' vertices having a 'description' property containing the token 'cold'.
   *
   * @test_category dse:graph
   */
  @UseDataProvider("indexTypes")
  @Test
  public void search_by_token(String indexType) {
    // Description containing token 'cold'
    GraphTraversal<Vertex, String> traversal =
        g.V()
            .has("user", "description_" + indexType, Search.token("cold"))
            .values("full_name_" + indexType);
    assertThat(traversal.toList()).containsOnly("Jill Alice", "George Bill Steve");
  }

  /**
   * Validates that a graph traversal can be made by using a Search token prefix predicate on an
   * indexed property of the given type.
   *
   * <p>Finds all 'user' vertices having a 'description' containing the token prefix 'h'.
   */
  @UseDataProvider("indexTypes")
  @Test
  public void search_by_token_prefix(String indexType) {
    // Description containing a token starting with h
    GraphTraversal<Vertex, String> traversal =
        g.V()
            .has("user", "description_" + indexType, Search.tokenPrefix("h"))
            .values("full_name_" + indexType);
    assertThat(traversal.toList()).containsOnly("Paul Thomas Joe", "James Paul Joe");
  }

  /**
   * Validates that a graph traversal can be made by using a Search token regex predicate on an
   * indexed property of the given type.
   *
   * <p>Finds all 'user' vertices having a 'description' containing the token regex
   * '(nice|hospital)'.
   */
  @UseDataProvider("indexTypes")
  @Test
  public void search_by_token_regex(String indexType) {
    // Description containing nice or hospital
    GraphTraversal<Vertex, String> traversal =
        g.V()
            .has("user", "description_" + indexType, Search.tokenRegex("(nice|hospital)"))
            .values("full_name_" + indexType);
    assertThat(traversal.toList()).containsOnly("Paul Thomas Joe", "Jill Alice");
  }

  /**
   * Validates that a graph traversal can be made by using a Search fuzzy predicate on an indexed
   * property of the given type.
   *
   * <p>Finds all 'user' vertices having a 'description' property matching 'lieks' with a fuzzy
   * distance of 1.
   *
   * @test_category dse:graph
   */
  @UseDataProvider("indexTypes")
  @Test
  @DseRequirement(min = "5.1.0")
  public void search_by_token_fuzzy(String indexType) {
    // Description containing 'lives' fuzzy
    GraphTraversal<Vertex, String> traversal =
        g.V()
            .has("user", "description_" + indexType, Search.tokenFuzzy("lieks", 1))
            .values("full_name_" + indexType);
    // Should not match 'Paul Thomas Joe' since description contains 'Lives' which is at distance of
    // 2 (e -> v, k -> e)
    // Should match 'James Paul Joe' since description contains 'Likes' (transpose e for k)
    assertThat(traversal.toList()).containsOnly("James Paul Joe");
  }

  /**
   * Validates that a graph traversal can be made by using a Search phrase predicate on an indexed
   * property of the given type.
   *
   * <p>Finds all 'user' vertices having a 'description' property matching 'a cold' with a distance
   * of 2.
   *
   * @test_category dse:graph
   */
  @UseDataProvider("indexTypes")
  @Test
  @DseRequirement(min = "5.1.0")
  public void search_by_phrase(String indexType) {
    // Full name contains phrase "Paul Joe"
    GraphTraversal<Vertex, String> traversal =
        g.V()
            .has("user", "description_" + indexType, Search.phrase("a cold", 2))
            .values("full_name_" + indexType);
    // Should match 'George Bill Steve' since 'A cold dude' is at distance of 0 for 'a cold'.
    // Should match 'Jill Alice' since 'Enjoys a very nice cold coca cola' is at distance of 2 for
    // 'a cold'.
    assertThat(traversal.toList()).containsOnly("George Bill Steve", "Jill Alice");
  }
}
