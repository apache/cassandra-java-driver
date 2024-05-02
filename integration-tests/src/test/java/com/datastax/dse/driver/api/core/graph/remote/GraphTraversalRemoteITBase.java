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
package com.datastax.dse.driver.api.core.graph.remote;

import static com.datastax.dse.driver.api.core.graph.TinkerGraphAssertions.assertThat;
import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.assertThatContainsLabel;
import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.assertThatContainsProperties;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.dse.driver.Assertions;
import com.datastax.dse.driver.api.core.graph.SocialTraversalSource;
import com.datastax.dse.driver.api.core.graph.TinkerPathAssert;
import com.datastax.dse.driver.api.core.graph.__;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.assertj.core.api.Assumptions;
import org.junit.Test;

public abstract class GraphTraversalRemoteITBase {

  protected abstract CqlSession session();

  protected abstract boolean isGraphBinary();

  protected abstract GraphTraversalSource graphTraversalSource();

  protected abstract SocialTraversalSource socialTraversalSource();

  protected abstract CustomCcmRule ccmRule();

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
    GraphTraversalSource g = graphTraversalSource();

    // given an existing vertex
    Vertex marko = g.V().hasLabel("person").has("name", "marko").next();
    if (isGraphBinary()) {
      Map<Object, Object> properties =
          g.V().hasLabel("person").has("name", "marko").elementMap("name").next();

      assertThat(properties).containsEntry("name", "marko");
    } else {
      assertThat(marko).hasProperty("name", "marko");
    }

    // then should be able to retrieve that same vertex by id.
    assertThat(g.V(marko.id()).next()).isEqualTo(marko);
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
  public void should_use_edge_is_as_parameter() {
    GraphTraversalSource g = graphTraversalSource();

    // given an existing edge
    Edge created = g.E().has("weight", 0.2f).next();

    if (isGraphBinary()) {
      List<Map<Object, Object>> properties =
          g.E().has("weight").elementMap("weight", "software", "person").toList();

      assertThat(properties)
          .anySatisfy(
              props -> {
                assertThatContainsProperties(props, "weight", 0.2f);
                assertThatContainsLabel(props, Direction.IN, "software");
                assertThatContainsLabel(props, Direction.OUT, "person");
              });

    } else {
      assertThat(created)
          .hasProperty("weight", 0.2f)
          .hasInVLabel("software")
          .hasOutVLabel("person");
    }

    // should be able to retrieve incoming and outgoing vertices by edge id
    if (isGraphBinary()) {
      Map<Object, Object> inProperties = g.E(created.id()).inV().elementMap("name", "lang").next();
      Map<Object, Object> outProperties = g.E(created.id()).outV().elementMap("name").next();
      assertThatContainsProperties(inProperties, "name", "lop", "lang", "java");
      assertThatContainsProperties(outProperties, "name", "peter");

    } else {
      Vertex in = g.E(created.id()).inV().next();
      Vertex out = g.E(created.id()).outV().next();

      // should resolve to lop
      assertThat(in).hasLabel("software").hasProperty("name", "lop").hasProperty("lang", "java");

      // should resolve to marko, josh and peter whom created lop.
      assertThat(out).hasLabel("person").hasProperty("name", "peter");
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
    GraphTraversalSource g = graphTraversalSource();
    // given an existing vertex
    Vertex marko = g.V().hasLabel("person").has("name", "marko").next();

    // then id should be a map with expected values.
    // Note: this is pretty dependent on DSE Graphs underlying id structure which may vary in
    // the future.
    if (isGraphBinary()) {
      assertThat(((String) marko.id())).contains("marko");
      assertThat(marko.label()).isEqualTo("person");
    } else {
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
   * org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal#by(Traversal)} with an
   * anonymous traversal to get inbound 'created' edges and folds them into a list.
   *
   * <p>Executes a vertex traversal that binds label 'a' and 'b' to vertex properties and label 'c'
   * to vertices that have edges from that vertex.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_handle_result_object_of_mixed_types() {
    GraphTraversalSource g = graphTraversalSource();
    // find all software vertices and select name, language, and find all vertices that created
    // such software.
    List<Map<String, Object>> results =
        g.V()
            .hasLabel("software")
            .as("a", "b", "c")
            .select("a", "b", "c")
            .by("name")
            .by("lang")
            .by(__.in("created").fold())
            .toList();

    // ensure that lop and ripple and their data are the results return.
    assertThat(results).extracting(m -> m.get("a")).containsOnly("lop", "ripple");

    for (Map<String, Object> result : results) {
      assertThat(result).containsOnlyKeys("a", "b", "c");
      // both software are written in java.
      assertThat(result.get("b")).isEqualTo("java");
      // ensure the created vertices match the creators of the software.
      @SuppressWarnings("unchecked")
      List<Vertex> vertices = (List<Vertex>) result.get("c");
      if (result.get("a").equals("lop")) {
        if (isGraphBinary()) {
          // should contain three vertices
          assertThat(vertices.size()).isEqualTo(3);
        } else {
          // lop, 'c' should contain marko, josh, peter.
          assertThat(vertices)
              .extracting(vertex -> vertex.property("name").value())
              .containsOnly("marko", "josh", "peter");
        }
      } else {
        if (isGraphBinary()) {
          // has only one label
          assertThat(vertices.size()).isEqualTo(1);
        } else {
          assertThat(vertices)
              .extracting(vertex -> vertex.property("name").value())
              .containsOnly("josh");
        }
      }
    }
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

    GraphTraversalSource g = graphTraversalSource();
    // retrieve a subgraph on the knows relationship, this omits the created edges.
    Graph graph = (Graph) g.E().hasLabel("knows").subgraph("subGraph").cap("subGraph").next();

    // there should only be 2 edges (since there are are only 2 knows relationships) and 3 vertices
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
  public void should_handle_subgraph_graph_binary() {
    Assumptions.assumeThat(isGraphBinary()).isTrue();

    GraphTraversalSource g = graphTraversalSource();
    // retrieve a subgraph on the knows relationship, this omits the created edges.
    String graph = (String) g.E().hasLabel("knows").subgraph("subGraph").cap("subGraph").next();

    // there should only be 2 edges (since there are are only 2 knows relationships) and 3 vertices
    assertThat(graph).contains("vertices:3").contains("edges:2");
  }

  /**
   * Ensures a traversal that yields no results is properly retrieved and is empty.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_return_zero_results() {
    if (isGraphBinary()) {
      assertThatThrownBy(() -> graphTraversalSource().V().hasLabel("notALabel").toList())
          .isInstanceOf(InvalidQueryException.class)
          .hasMessageContaining("Unknown vertex label 'notALabel'");
    } else {
      assertThat(graphTraversalSource().V().hasLabel("notALabel").toList()).isEmpty();
    }
  }

  /**
   * Validates that a traversal returning a {@link Tree} structure is returned appropriately with
   * the expected contents.
   *
   * <p>Retrieves trees of people marko knows and the software they created.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_parse_tree() {
    // Get a tree structure showing the paths from mark to people he knows to software they've
    // created.
    @SuppressWarnings("unchecked")
    Tree<String> tree =
        graphTraversalSource()
            .V()
            .hasLabel("person")
            .out("knows")
            .out("created")
            .tree()
            .by("name")
            .next();

    // Marko knows josh who created lop and ripple.
    assertThat(tree).tree("marko").tree("josh").tree("lop").isLeaf();

    assertThat(tree).tree("marko").tree("josh").tree("ripple").isLeaf();
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
    List<Object> software =
        graphTraversalSource()
            .V()
            .hasLabel("person")
            .filter(__.has("name", "marko"))
            .out("knows")
            .flatMap(__.out("created"))
            .map(__.values("name"))
            .fold()
            .next();

    // Marko only knows josh and vadas, of which josh created lop and ripple.
    assertThat(software).containsOnly("lop", "ripple");
  }

  /**
   * Validates that {@link
   * org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal#tryNext()} functions
   * appropriate by returning an {@link Optional} of which the presence of the underlying data
   * depends on whether or not remaining data is present.
   *
   * <p>This is more of a test of Tinkerpop than the protocol between the client and DSE graph.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_handle_tryNext() {
    GraphTraversal<Vertex, Vertex> traversal =
        graphTraversalSource().V().hasLabel("person").has("name", "marko");

    // value present
    Optional<Vertex> v0 = traversal.tryNext();
    assertThat(v0.isPresent()).isTrue();
    if (!isGraphBinary()) {
      assertThat(v0.get()).hasProperty("name", "marko");
    }

    // value absent as there was only 1 matching vertex.
    Optional<Vertex> v1 = traversal.tryNext();
    assertThat(v1.isPresent()).isFalse();
  }

  /**
   * Validates that {@link GraphTraversal#toStream()} appropriately creates a stream from the
   * underlying iterator on the traversal, and then an attempt to call toStream again yields no
   * results.
   *
   * <p>This is more of a test of Tinkerpop than the protocol between the client and DSE graph.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_handle_streaming_graphson() {
    Assumptions.assumeThat(isGraphBinary()).isFalse();

    GraphTraversal<Vertex, Vertex> traversal = graphTraversalSource().V().hasLabel("person");
    // retrieve all person vertices to stream, and filter on client side all persons under age 30
    // and map to their name.
    List<String> under30 =
        traversal
            .toStream()
            .filter(v -> v.<Integer>property("age").value() < 30)
            .map(v -> v.<String>property("name").value())
            .collect(Collectors.toList());

    assertThat(under30).containsOnly("marko", "vadas");

    // attempt to get a stream again, which should be empty.
    assertThat(traversal.toStream().collect(Collectors.toList())).isEmpty();
  }

  /**
   * Validates that {@link GraphTraversal#toStream()} appropriately creates a stream from the
   * underlying iterator on the traversal, and then an attempt to call toStream again yields no
   * results.
   *
   * <p>This is more of a test of Tinkerpop than the protocol between the client and DSE graph.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_handle_streaming_binary() {
    Assumptions.assumeThat(isGraphBinary()).isTrue();

    GraphTraversal<Vertex, Map<Object, Object>> traversal =
        graphTraversalSource().V().hasLabel("person").elementMap("age", "name");
    // retrieve all person vertices to stream, and filter on client side all persons under age 30
    // and map to their name.
    List<String> under30 =
        traversal
            .toStream()
            .filter(v -> (Integer) v.get("age") < 30)
            .map(v -> (String) v.get("name"))
            .collect(Collectors.toList());

    assertThat(under30).containsOnly("marko", "vadas");

    // attempt to get a stream again, which should be empty.
    assertThat(traversal.toStream().collect(Collectors.toList())).isEmpty();
  }

  /**
   * Validates that when traversing a path and labeling some of the elements during the traversal
   * that the output elements are properly labeled.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_resolve_path_with_some_labels() {
    // given a traversal where some objects have labels.
    List<Path> paths =
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
            .path()
            .toList();

    // then the paths returned should be labeled for the
    // appropriate objects, and not labeled otherwise.
    for (Path path : paths) {
      TinkerPathAssert.validatePathObjects(path);
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
    // given a traversal where all objects have labels.
    List<Path> paths =
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
            .path()
            .toList();

    // then the paths returned should be labeled for all
    // objects.
    for (Path path : paths) {
      TinkerPathAssert.validatePathObjects(path);
      Assertions.assertThat(path.labels()).hasSize(5);
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
    // given a traversal where no objects have labels.
    List<Path> paths =
        graphTraversalSource()
            .V()
            .hasLabel("person")
            .has("name", "marko")
            .outE("knows")
            .inV()
            .outE("created")
            .inV()
            .path()
            .toList();

    // then the paths returned should be labeled for
    // all objects.
    for (Path path : paths) {
      TinkerPathAssert.validatePathObjects(path);
      for (int i = 0; i < 5; i++) assertThat(path).hasNoLabel(i);
    }
  }

  @Test
  public void should_handle_asynchronous_execution_graphson() {
    Assumptions.assumeThat(isGraphBinary()).isFalse();

    StringBuilder names = new StringBuilder();

    CompletableFuture<List<Vertex>> future =
        graphTraversalSource().V().hasLabel("person").promise(Traversal::toList);
    try {
      // dumb processing to make sure the completable future works correctly and correct results are
      // returned
      future
          .thenAccept(
              vertices -> vertices.forEach(vertex -> names.append((String) vertex.value("name"))))
          .get();
    } catch (InterruptedException | ExecutionException e) {
      fail("Shouldn't have thrown an exception waiting for the result to complete");
    }

    assertThat(names.toString()).contains("peter", "marko", "vadas", "josh");
  }

  @Test
  public void should_handle_asynchronous_execution_graph_binary() {
    Assumptions.assumeThat(isGraphBinary()).isTrue();

    StringBuilder names = new StringBuilder();

    CompletableFuture<List<Vertex>> future =
        graphTraversalSource().V().hasLabel("person").promise(Traversal::toList);
    try {
      // dumb processing to make sure the completable future works correctly and correct results are
      // returned
      future.thenAccept(vertices -> vertices.forEach(vertex -> names.append(vertex.id()))).get();
    } catch (InterruptedException | ExecutionException e) {
      fail("Shouldn't have thrown an exception waiting for the result to complete");
    }

    assertThat(names.toString()).contains("peter", "marko", "vadas", "josh");
  }

  /**
   * Validates that if a traversal is made that encounters an error on the server side that the
   * exception is set on the future.
   *
   * @test_category dse:graph
   */
  @Test
  @BackendRequirement(type = BackendType.DSE, minInclusive = "5.1.0")
  public void should_fail_future_returned_from_promise_on_query_error() throws Exception {
    CompletableFuture<?> future =
        graphTraversalSource().V("invalidid").peerPressure().promise(Traversal::next);

    try {
      future.get();
      fail("Expected an ExecutionException");
    } catch (ExecutionException e) {
      assertThat(e.getCause()).isInstanceOf(InvalidQueryException.class);
    }
  }

  /**
   * A simple smoke test to ensure that a user can supply a custom {@link GraphTraversalSource} for
   * use with DSLs.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_allow_use_of_dsl_graphson() {
    Assumptions.assumeThat(isGraphBinary()).isFalse();

    List<Vertex> vertices = socialTraversalSource().persons("marko").knows("vadas").toList();
    assertThat(vertices.size()).isEqualTo(1);
    assertThat(vertices.get(0))
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
  public void should_allow_use_of_dsl_graph_binary() {
    Assumptions.assumeThat(isGraphBinary()).isTrue();

    List<Map<Object, Object>> vertices =
        socialTraversalSource().persons("marko").knows("vadas").elementMap("name", "age").toList();
    assertThat(vertices.size()).isEqualTo(1);

    assertThatContainsProperties(vertices.get(0), "name", "marko", "age", 29);
    assertThat(vertices.get(0).values()).contains("person");
  }

  /**
   * Ensures that traversals with barriers (which return results bulked) contain the correct amount
   * of end results.
   *
   * <p>This will fail if ran against DSE < 5.0.9 or DSE < 5.1.2.
   */
  @Test
  public void should_return_correct_results_when_bulked() {
    Optional<Version> dseVersion = ccmRule().getCcmBridge().getDseVersion();
    Assumptions.assumeThat(
            dseVersion.isPresent() && dseVersion.get().compareTo(Version.parse("5.1.2")) > 0)
        .isTrue();

    List<String> results = graphTraversalSource().E().label().barrier().toList();
    Collections.sort(results);

    List<String> expected =
        Arrays.asList("knows", "created", "created", "knows", "created", "created");
    Collections.sort(expected);

    assertThat(results).isEqualTo(expected);
  }
}
