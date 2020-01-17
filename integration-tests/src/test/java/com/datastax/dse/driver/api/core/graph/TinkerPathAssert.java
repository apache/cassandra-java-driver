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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractObjectAssert;

public class TinkerPathAssert extends AbstractAssert<TinkerPathAssert, Path> {

  public TinkerPathAssert(Path actual) {
    super(actual, TinkerPathAssert.class);
  }

  /**
   * Ensures that the given Path matches one of the exact traversals we'd expect for a person whom
   * Marko knows that has created software and what software that is.
   *
   * <p>These paths should be:
   *
   * <ul>
   *   <li>marko -> knows -> josh -> created -> lop
   *   <li>marko -> knows -> josh -> created -> ripple
   * </ul>
   */
  public static void validatePathObjects(Path path) {

    // marko should be the origin point.
    TinkerGraphAssertions.assertThat(path).vertexAt(0).hasLabel("person");

    // there should be a 'knows' outgoing relationship between marko and josh.
    TinkerGraphAssertions.assertThat(path)
        .edgeAt(1)
        .hasLabel("knows")
        .hasOutVLabel("person")
        .hasOutV((Vertex) path.objects().get(0))
        .hasInVLabel("person")
        .hasInV((Vertex) path.objects().get(2));

    // josh...
    TinkerGraphAssertions.assertThat(path).vertexAt(2).hasLabel("person");

    // there should be a 'created' relationship between josh and lop.
    TinkerGraphAssertions.assertThat(path)
        .edgeAt(3)
        .hasLabel("created")
        .hasOutVLabel("person")
        .hasOutV((Vertex) path.objects().get(2))
        .hasInVLabel("software")
        .hasInV((Vertex) path.objects().get(4));

    // lop..
    TinkerGraphAssertions.assertThat(path).vertexAt(4).hasLabel("software");
  }

  public AbstractObjectAssert<?, Object> objectAt(int i) {
    assertThat(actual.size()).isGreaterThanOrEqualTo(i);
    return assertThat(actual.objects().get(i));
  }

  public TinkerVertexAssert vertexAt(int i) {
    assertThat(actual.size()).isGreaterThanOrEqualTo(i);
    Object o = actual.objects().get(i);
    assertThat(o).isInstanceOf(Vertex.class);
    return new TinkerVertexAssert((Vertex) o);
  }

  public TinkerEdgeAssert edgeAt(int i) {
    assertThat(actual.size()).isGreaterThanOrEqualTo(i);
    Object o = actual.objects().get(i);
    assertThat(o).isInstanceOf(Edge.class);
    return new TinkerEdgeAssert((Edge) o);
  }

  public TinkerPathAssert hasLabel(int i, String... labels) {
    assertThat(actual.labels().size()).isGreaterThanOrEqualTo(i);
    assertThat(actual.labels().get(i)).containsExactly(labels);
    return myself;
  }

  public TinkerPathAssert hasNoLabel(int i) {
    assertThat(actual.labels().size()).isGreaterThanOrEqualTo(i);
    assertThat(actual.labels().get(i)).isEmpty();
    return myself;
  }

  public TinkerPathAssert doesNotHaveLabel(String label) {
    assertThat(actual.hasLabel(label)).isFalse();
    return myself;
  }
}
