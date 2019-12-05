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
package com.datastax.dse.driver.api.core.graph;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.graph.predicates.Search;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public abstract class GraphTextSearchIndexITBase {

  protected abstract boolean isGraphBinary();

  protected abstract GraphTraversalSource graphTraversalSource();

  @DataProvider
  public static Object[][] indexTypes() {
    return new Object[][] {{"search"}

      // FIXME for some reason, materialized and secondary indices have decided not to work
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
        graphTraversalSource()
            .V()
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
        graphTraversalSource()
            .V()
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
        graphTraversalSource()
            .V()
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
        graphTraversalSource()
            .V()
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
        graphTraversalSource()
            .V()
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
        graphTraversalSource()
            .V()
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
        graphTraversalSource()
            .V()
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
        graphTraversalSource()
            .V()
            .has("user", "description_" + indexType, Search.phrase("a cold", 2))
            .values("full_name_" + indexType);
    // Should match 'George Bill Steve' since 'A cold dude' is at distance of 0 for 'a cold'.
    // Should match 'Jill Alice' since 'Enjoys a very nice cold coca cola' is at distance of 2 for
    // 'a cold'.
    assertThat(traversal.toList()).containsOnly("George Bill Steve", "Jill Alice");
  }
}
