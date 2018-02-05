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
package com.datastax.oss.driver.internal.core.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class DirectedGraphTest {

  @Test
  public void should_sort_empty_graph() {
    DirectedGraph<String> g = new DirectedGraph<>();
    assertThat(g.topologicalSort()).isEmpty();
  }

  @Test
  public void should_sort_graph_with_one_node() {
    DirectedGraph<String> g = new DirectedGraph<>("A");
    assertThat(g.topologicalSort()).containsExactly("A");
  }

  @Test
  public void should_sort_complex_graph() {
    //         H   G
    //        / \ /\
    //       F   |  E
    //        \ /  /
    //         D  /
    //        / \/
    //        B  C
    //        |
    //        A
    DirectedGraph<String> g = new DirectedGraph<>("A", "B", "C", "D", "E", "F", "G", "H");
    g.addEdge("H", "F");
    g.addEdge("G", "E");
    g.addEdge("H", "D");
    g.addEdge("F", "D");
    g.addEdge("G", "D");
    g.addEdge("D", "C");
    g.addEdge("E", "C");
    g.addEdge("D", "B");
    g.addEdge("B", "A");

    // The graph uses linked hash maps internally, so this order will be consistent across JVMs
    assertThat(g.topologicalSort()).containsExactly("G", "H", "E", "F", "D", "C", "B", "A");
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_sort_if_graph_has_a_cycle() {
    DirectedGraph<String> g = new DirectedGraph<>("A", "B", "C");
    g.addEdge("A", "B");
    g.addEdge("B", "C");
    g.addEdge("C", "B");

    g.topologicalSort();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_sort_if_graph_is_a_cycle() {
    DirectedGraph<String> g = new DirectedGraph<>("A", "B", "C");
    g.addEdge("A", "B");
    g.addEdge("B", "C");
    g.addEdge("C", "A");

    g.topologicalSort();
  }
}
