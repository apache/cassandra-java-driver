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
package com.datastax.dse.driver.api.core.graph.statement;

import static com.datastax.dse.driver.api.core.graph.FluentGraphStatement.newInstance;
import static com.datastax.dse.driver.api.core.graph.TinkerGraphAssertions.assertThat;
import static com.datastax.dse.driver.api.core.graph.TinkerPathAssert.validatePathObjects;
import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.assertThatContainsLabel;
import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.assertThatContainsProperties;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.api.core.graph.SocialTraversalSource;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.Test;

public abstract class GraphTraversalITBase {

  protected abstract CqlSession session();

  protected abstract boolean isGraphBinary();

  protected abstract CustomCcmRule ccmRule();

  protected abstract GraphTraversalSource graphTraversalSource();

  protected abstract SocialTraversalSource socialTraversalSource();

  /**
   * Ensures that a previously returned {@link Vertex}'s {@link Vertex#id()} can be used as an input
   * to {@link
   * org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource#V(Object...)} to
   * retrieve the {@link Vertex} and that the returned {@link Vertex} is the same.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_use_vertex_id_as_parameter() {
    GraphTraversal<Vertex, Vertex> query =
        graphTraversalSource().V().hasLabel("person").has("name", "marko");
    GraphResultSet resultSet = session().execute(newInstance(query));

    List<GraphNode> results = resultSet.all();

    assertThat(results.size()).isEqualTo(1);
    Vertex marko = results.get(0).asVertex();
    if (isGraphBinary()) {
      Map<Object, Object> properties =
          session().execute(newInstance(query.elementMap("name"))).one().asMap();
      assertThatContainsProperties(properties, "name", "marko");
    } else {
      assertThat(marko).hasProperty("name", "marko");
    }

    resultSet = session().execute(newInstance(graphTraversalSource().V(marko.id())));

    results = resultSet.all();
    assertThat(results.size()).isEqualTo(1);
    Vertex marko2 = results.get(0).asVertex();
    // Ensure that the returned vertex is the same as the first.
    assertThat(marko2).isEqualTo(marko);
  }

  /**
   * Ensures that a previously returned {@link Edge}'s {@link Edge#id()} can be used as an input to
   * {@link
   * org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource#E(Object...)} to
   * retrieve the {@link Edge} and that the returned {@link Edge} is the same.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_use_edge_id_as_parameter() {
    GraphTraversal<Edge, Edge> query = graphTraversalSource().E().has("weight", 0.2f);
    GraphResultSet resultSet = session().execute(newInstance(query));

    List<GraphNode> results = resultSet.all();
    assertThat(results.size()).isEqualTo(1);

    Edge created = results.get(0).asEdge();
    if (isGraphBinary()) {
      Map<Object, Object> properties =
          session()
              .execute(newInstance(query.elementMap("weight", "software", "person")))
              .one()
              .asMap();

      assertThatContainsProperties(properties, "weight", 0.2f);
      assertThatContainsLabel(properties, Direction.IN, "software");
      assertThatContainsLabel(properties, Direction.OUT, "person");
    } else {
      assertThat(created)
          .hasProperty("weight", 0.2f)
          .hasInVLabel("software")
          .hasOutVLabel("person");
    }

    if (isGraphBinary()) {
      Map<Object, Object> inProperties =
          session()
              .execute(
                  newInstance(
                      graphTraversalSource().E(created.id()).inV().elementMap("name", "lang")))
              .one()
              .asMap();
      assertThatContainsProperties(inProperties, "name", "lop", "lang", "java");
    } else {
      resultSet = session().execute(newInstance(graphTraversalSource().E(created.id()).inV()));
      results = resultSet.all();
      assertThat(results.size()).isEqualTo(1);
      Vertex lop = results.get(0).asVertex();

      assertThat(lop).hasLabel("software").hasProperty("name", "lop").hasProperty("lang", "java");
    }
  }

  /**
   * A sanity check that a returned {@link Vertex}'s id is a {@link Map}. This test could break in
   * the future if the format of a vertex ID changes from a Map to something else in DSE.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_deserialize_vertex_id_as_map() {
    GraphResultSet resultSet =
        session()
            .execute(
                newInstance(graphTraversalSource().V().hasLabel("person").has("name", "marko")));

    List<GraphNode> results = resultSet.all();
    assertThat(results.size()).isEqualTo(1);

    Vertex marko = results.get(0).asVertex();

    if (isGraphBinary()) {
      assertThat(((String) marko.id())).contains("marko");
      assertThat(marko.label()).isEqualTo("person");
    } else {
      assertThat(marko).hasProperty("name", "marko");
      @SuppressWarnings("unchecked")
      Map<String, String> id = (Map<String, String>) marko.id();
      assertThat(id)
          .hasSize(3)
          .containsEntry("~label", "person")
          .containsKey("community_id")
          .containsKey("member_id");
    }
  }

  /**
   * Ensures that a traversal that returns a result of mixed types is interpreted as a {@link Map}
   * with {@link Object} values. Also uses {@link
   * org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal#by(org.apache.tinkerpop.gremlin.process.traversal.Traversal)}
   * with an anonymous traversal to get inbound 'created' edges and folds them into a list.
   *
   * <p>Executes a vertex traversal that binds label 'a' and 'b' to vertex properties and label 'c'
   * to vertices that have edges from that vertex.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_handle_result_object_of_mixed_types() {
    // find all software vertices and select name, language, and find all vertices that created such
    // software.
    GraphResultSet rs =
        session()
            .execute(
                newInstance(
                    graphTraversalSource()
                        .V()
                        .hasLabel("software")
                        .as("a", "b", "c")
                        .select("a", "b", "c")
                        .by("name")
                        .by("lang")
                        .by(__.in("created").fold())));

    List<GraphNode> results = rs.all();
    assertThat(results.size()).isEqualTo(2);

    // Ensure that we got 'lop' and 'ripple' for property a.
    assertThat(results)
        .extracting(m -> m.getByKey("a").as(Object.class))
        .containsOnly("lop", "ripple");

    for (GraphNode result : results) {
      // The row should represent a map with a, b, and c keys.
      assertThat(ImmutableList.<Object>copyOf(result.keys())).containsOnlyOnce("a", "b", "c");
      // 'e' should not exist, thus it should be null.
      assertThat(result.getByKey("e")).isNull();
      // both software are written in java.
      assertThat(result.getByKey("b").isNull()).isFalse();
      assertThat(result.getByKey("b").asString()).isEqualTo("java");
      GraphNode c = result.getByKey("c");
      assertThat(c.isList()).isTrue();
      if (result.getByKey("a").asString().equals("lop")) {
        if (isGraphBinary()) {
          // should contain three vertices
          Assertions.assertThat(c.size()).isEqualTo(3);
        } else {
          // 'c' should contain marko, josh, peter.
          // Ensure we have three vertices.
          assertThat(c.size()).isEqualTo(3);
          List<Vertex> vertices =
              Lists.newArrayList(
                  c.getByIndex(0).asVertex(),
                  c.getByIndex(1).asVertex(),
                  c.getByIndex(2).asVertex());
          assertThat(vertices)
              .extracting(vertex -> vertex.property("name").value())
              .containsOnly("marko", "josh", "peter");
        }
      } else {
        if (isGraphBinary()) {
          // has only one label
          Assertions.assertThat(c.size()).isEqualTo(1);
        } else {
          // ripple, 'c' should contain josh.
          // Ensure we have 1 vertex.
          assertThat(c.size()).isEqualTo(1);
          Vertex vertex = c.getByIndex(0).asVertex();
          assertThat(vertex).hasProperty("name", "josh");
        }
      }
    }
  }

  /**
   * Ensures a traversal that yields no results is properly retrieved and is empty.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_return_zero_results() {
    if (isGraphBinary()) {
      assertThatThrownBy(
              () ->
                  session().execute(newInstance(graphTraversalSource().V().hasLabel("notALabel"))))
          .isInstanceOf(InvalidQueryException.class)
          .hasMessageContaining("Unknown vertex label 'notALabel'");
    } else {
      GraphResultSet rs =
          session().execute(newInstance(graphTraversalSource().V().hasLabel("notALabel")));
      assertThat(rs.all().size()).isZero();
    }
  }

  /**
   * Ensures a traversal that yields no results is properly retrieved and is empty, using GraphSON2
   * and the TinkerPop transform results function.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_return_zero_results_graphson_2() {
    Assumptions.assumeThat(isGraphBinary()).isFalse();

    GraphStatement simpleGraphStatement =
        ScriptGraphStatement.newInstance("g.V().hasLabel('notALabel')");

    GraphResultSet rs = session().execute(simpleGraphStatement);
    assertThat(rs.one()).isNull();
  }

  /**
   * Validates that a traversal using lambda operations with anonymous traversals are applied
   * appropriately and return the expected results.
   *
   * <p>Traversal that filters 'person'-labeled vertices by name 'marko' and flatMaps outgoing
   * vertices on the 'knows' relationship by their outgoing 'created' vertices and then maps by
   * their 'name' property and folds them into one list.
   *
   * <p><b>Note:</b> This does not validate lambdas with functions as those can't be interpreted and
   * sent remotely.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_handle_lambdas() {
    // Find all people marko knows and the software they created.
    GraphResultSet result =
        session()
            .execute(
                newInstance(
                    graphTraversalSource()
                        .V()
                        .hasLabel("person")
                        .filter(__.has("name", "marko"))
                        .out("knows")
                        .flatMap(__.out("created"))
                        .map(__.values("name"))
                        .fold()));

    // Marko only knows josh and vadas, of which josh created lop and ripple.
    List<String> software = result.one().as(GenericType.listOf(String.class));
    assertThat(software).containsOnly("lop", "ripple");
  }

  /**
   * Validates that when traversing a path and labeling some of the elements during the traversal
   * that the output elements are properly labeled.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_resolve_path_with_some_labels() {
    GraphResultSet rs =
        session()
            .execute(
                newInstance(
                    graphTraversalSource()
                        .V()
                        .hasLabel("person")
                        .has("name", "marko")
                        .as("a")
                        .outE("knows")
                        .inV()
                        .as("c", "d")
                        .outE("created")
                        .as("e", "f", "g")
                        .inV()
                        .path()));

    List<GraphNode> results = rs.all();
    assertThat(results.size()).isEqualTo(2);
    for (GraphNode result : results) {
      Path path = result.asPath();
      validatePathObjects(path);
      assertThat(path.labels()).hasSize(5);
      assertThat(path)
          .hasLabel(0, "a")
          .hasNoLabel(1)
          .hasLabel(2, "c", "d")
          .hasLabel(3, "e", "f", "g")
          .hasNoLabel(4);
    }
  }

  /**
   * Validates that when traversing a path and labeling all of the elements during the traversal
   * that the output elements are properly labeled.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_resolve_path_with_labels() {
    GraphResultSet rs =
        session()
            .execute(
                newInstance(
                    graphTraversalSource()
                        .V()
                        .hasLabel("person")
                        .has("name", "marko")
                        .as("a")
                        .outE("knows")
                        .as("b")
                        .inV()
                        .as("c", "d")
                        .outE("created")
                        .as("e", "f", "g")
                        .inV()
                        .as("h")
                        .path()));
    List<GraphNode> results = rs.all();
    assertThat(results.size()).isEqualTo(2);
    for (GraphNode result : results) {
      Path path = result.asPath();
      validatePathObjects(path);
      assertThat(path.labels()).hasSize(5);
      assertThat(path)
          .hasLabel(0, "a")
          .hasLabel(1, "b")
          .hasLabel(2, "c", "d")
          .hasLabel(3, "e", "f", "g")
          .hasLabel(4, "h");
    }
  }

  /**
   * Validates that when traversing a path and labeling none of the elements during the traversal
   * that all the labels are empty in the result.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_resolve_path_without_labels() {
    GraphResultSet rs =
        session()
            .execute(
                newInstance(
                    graphTraversalSource()
                        .V()
                        .hasLabel("person")
                        .has("name", "marko")
                        .outE("knows")
                        .inV()
                        .outE("created")
                        .inV()
                        .path()));
    List<GraphNode> results = rs.all();
    assertThat(results.size()).isEqualTo(2);
    for (GraphNode result : results) {
      Path path = result.asPath();
      validatePathObjects(path);
      assertThat(path.labels()).hasSize(5);
      for (int i = 0; i < 5; i++) assertThat(path).hasNoLabel(i);
    }
  }

  /**
   * Validates that a traversal returning a Tree structure is returned appropriately with the
   * expected contents.
   *
   * <p>Retrieves trees of people marko knows and the software they created.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_parse_tree() {
    // Get a tree structure showing the paths from mark to people he knows to software they've
    // created.
    GraphResultSet rs =
        session()
            .execute(
                newInstance(
                    graphTraversalSource()
                        .V()
                        .hasLabel("person")
                        .out("knows")
                        .out("created")
                        .tree()
                        .by("name")));

    List<GraphNode> results = rs.all();
    assertThat(results.size()).isEqualTo(1);

    // [{key=marko, value=[{key=josh, value=[{key=ripple, value=[]}, {key=lop, value=[]}]}]}]
    GraphNode result = results.get(0);

    @SuppressWarnings("unchecked")
    Tree<String> tree = result.as(Tree.class);

    assertThat(tree).tree("marko").tree("josh").tree("lop").isLeaf();

    assertThat(tree).tree("marko").tree("josh").tree("ripple").isLeaf();
  }

  /**
   * Ensures that a traversal that returns a sub graph can be retrieved.
   *
   * <p>The subgraph is all members in a knows relationship, thus is all people who marko knows and
   * the edges that connect them.
   */
  @Test
  public void should_handle_subgraph_graphson() {
    Assumptions.assumeThat(isGraphBinary()).isFalse();
    GraphResultSet rs =
        session()
            .execute(
                newInstance(
                    graphTraversalSource()
                        .E()
                        .hasLabel("knows")
                        .subgraph("subGraph")
                        .cap("subGraph")));

    List<GraphNode> results = rs.all();
    assertThat(results.size()).isEqualTo(1);

    Graph graph = results.get(0).as(Graph.class);

    assertThat(graph.edges()).toIterable().hasSize(2);
    assertThat(graph.vertices()).toIterable().hasSize(3);
  }

  /**
   * Ensures that a traversal that returns a sub graph can be retrieved.
   *
   * <p>The subgraph is all members in a knows relationship, thus is all people who marko knows and
   * the edges that connect them.
   */
  @Test
  public void should_handle_subgraph_grap_binary() {
    Assumptions.assumeThat(isGraphBinary()).isTrue();
    GraphResultSet rs =
        session()
            .execute(
                newInstance(
                    graphTraversalSource()
                        .E()
                        .hasLabel("knows")
                        .subgraph("subGraph")
                        .cap("subGraph")));

    List<GraphNode> results = rs.all();
    assertThat(results.size()).isEqualTo(1);

    String graph = results.get(0).as(String.class);

    assertThat(graph).contains("vertices:3").contains("edges:2");
  }

  /**
   * A simple smoke test to ensure that a user can supply a custom {@link GraphTraversalSource} for
   * use with DSLs.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_allow_use_of_dsl_graphson() throws Exception {
    Assumptions.assumeThat(isGraphBinary()).isFalse();
    SocialTraversalSource gSocial = socialTraversalSource();

    GraphStatement gs = newInstance(gSocial.persons("marko").knows("vadas"));

    GraphResultSet rs = session().execute(gs);
    List<GraphNode> results = rs.all();

    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).asVertex())
        .hasProperty("name", "marko")
        .hasProperty("age", 29)
        .hasLabel("person");
  }

  /**
   * A simple smoke test to ensure that a user can supply a custom {@link GraphTraversalSource} for
   * use with DSLs.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_allow_use_of_dsl_graph_binary() throws Exception {
    Assumptions.assumeThat(isGraphBinary()).isTrue();
    SocialTraversalSource gSocial = socialTraversalSource();

    GraphStatement gs =
        newInstance(gSocial.persons("marko").knows("vadas").elementMap("name", "age"));

    GraphResultSet rs = session().execute(gs);
    List<GraphNode> results = rs.all();

    assertThat(results.size()).isEqualTo(1);
    assertThatContainsProperties(results.get(0).asMap(), "name", "marko", "age", 29);
    Assertions.assertThat(results.get(0).asMap().values()).contains("person");
  }

  /**
   * Ensures that traversals with barriers (which return results bulked) contain the correct amount
   * of end results.
   *
   * <p>This will fail if ran against DSE < 5.0.9 or DSE < 5.1.2.
   */
  @Test
  public void should_return_correct_results_when_bulked() {
    Assumptions.assumeThat(
            CcmBridge.isDistributionOf(
                BackendType.DSE, (dist, cass) -> dist.compareTo(Version.parse("5.1.2")) > 0))
        .isTrue();

    GraphResultSet rs =
        session().execute(newInstance(graphTraversalSource().E().label().barrier()));

    List<String> results =
        rs.all().stream().map(GraphNode::asString).sorted().collect(Collectors.toList());

    assertThat(results)
        .hasSize(6)
        .containsSequence("created", "created", "created", "created")
        .containsSequence("knows", "knows");
  }

  @Test
  public void should_handle_asynchronous_execution_graphson() {
    Assumptions.assumeThat(isGraphBinary()).isFalse();
    StringBuilder names = new StringBuilder();

    CompletionStage<AsyncGraphResultSet> future =
        session()
            .executeAsync(
                FluentGraphStatement.newInstance(graphTraversalSource().V().hasLabel("person")));

    try {
      // dumb processing to make sure the completable future works correctly and correct results are
      // returned
      Iterable<GraphNode> results =
          future.thenApply(AsyncGraphResultSet::currentPage).toCompletableFuture().get();
      for (GraphNode gn : results) {
        names.append(gn.asVertex().property("name").value());
      }
    } catch (InterruptedException | ExecutionException e) {
      fail("Shouldn't have thrown an exception waiting for the result to complete");
    }

    assertThat(names.toString()).contains("peter", "marko", "vadas", "josh");
  }

  @Test
  public void should_handle_asynchronous_execution_graph_binary() {
    Assumptions.assumeThat(isGraphBinary()).isTrue();
    StringBuilder names = new StringBuilder();

    CompletionStage<AsyncGraphResultSet> future =
        session()
            .executeAsync(
                FluentGraphStatement.newInstance(graphTraversalSource().V().hasLabel("person")));

    try {
      // dumb processing to make sure the completable future works correctly and correct results are
      // returned
      Iterable<GraphNode> results =
          future.thenApply(AsyncGraphResultSet::currentPage).toCompletableFuture().get();
      for (GraphNode gn : results) {
        names.append(gn.asVertex().id());
      }
    } catch (InterruptedException | ExecutionException e) {
      fail("Shouldn't have thrown an exception waiting for the result to complete");
    }

    assertThat(names.toString()).contains("peter", "marko", "vadas", "josh");
  }
}
