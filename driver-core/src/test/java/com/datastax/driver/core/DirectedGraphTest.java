/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.DriverInternalError;
import java.util.Comparator;
import java.util.List;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DirectedGraphTest {

    private Comparator<String> alphaComparator = new Comparator<String>() {

        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    };

    @Test(groups = "unit")
    public void should_sort_empty_graph() {
        DirectedGraph<String> g = new DirectedGraph<String>(alphaComparator);
        assertThat(g.topologicalSort()).isEmpty();
    }

    @Test(groups = "unit")
    public void should_sort_graph_with_one_node() {
        DirectedGraph<String> g = new DirectedGraph<String>(alphaComparator, "A");
        assertThat(g.topologicalSort())
                .containsExactly("A");
    }

    @Test(groups = "unit")
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
        DirectedGraph<String> g = new DirectedGraph<String>(alphaComparator, "A", "B", "C", "D", "E", "F", "G", "H");
        g.addEdge("H", "F");
        g.addEdge("G", "E");
        g.addEdge("H", "D");
        g.addEdge("F", "D");
        g.addEdge("G", "D");
        g.addEdge("D", "C");
        g.addEdge("E", "C");
        g.addEdge("D", "B");
        g.addEdge("B", "A");

        // Topological sort order should be : GH,E,F,D,BC,A
        List<String> sorted = g.topologicalSort();
        assertThat(sorted).containsExactly("G", "H", "E", "F", "D", "B", "C", "A");
    }

    @Test(groups = "unit")
    public void should_sort_complex_custom_comparator() {
        // Version of should_sort_complex_graph using a custom comparator based on ordering largest values first.
        // This is counter to how hashmaps should usually behave, so this should help ensure that the comparator is
        // being used.
        Comparator<Integer> highFirst = new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o2 - o1;
            }
        };

        // sort graph and use a alphaComparator that favors larger values ordered first.
        //         7   6
        //        / \ /\
        //       5   | 10
        //        \ /  /
        //         9  /
        //        / \/
        //        1  2
        //        |
        //        0
        DirectedGraph<Integer> g = new DirectedGraph<Integer>(highFirst, 0, 1, 2, 9, 10, 5, 6, 7);
        g.addEdge(7, 5);
        g.addEdge(6, 10);
        g.addEdge(7, 9);
        g.addEdge(5, 9);
        g.addEdge(6, 9);
        g.addEdge(9, 2);
        g.addEdge(10, 2);
        g.addEdge(9, 1);
        g.addEdge(1, 0);

        // Topological sort order should be : [7,6],[5],[10],[9],[2,1],[0]
        // 5 comes before 10 even though they appear at the same depth.  This happens because 5's (7) dependency
        // is evaluated before 10's (6), so it is placed first.
        List<Integer> sorted = g.topologicalSort();
        assertThat(sorted).containsExactly(7, 6, 5, 10, 9, 2, 1, 0);
    }

    @Test(groups = "unit", expectedExceptions = DriverInternalError.class)
    public void should_fail_to_sort_if_graph_has_a_cycle() {
        DirectedGraph<String> g = new DirectedGraph<String>(alphaComparator, "A", "B", "C");
        g.addEdge("A", "B");
        g.addEdge("B", "C");
        g.addEdge("C", "B");

        g.topologicalSort();
    }

    @Test(groups = "unit", expectedExceptions = DriverInternalError.class)
    public void should_fail_to_sort_if_graph_is_a_cycle() {
        DirectedGraph<String> g = new DirectedGraph<String>(alphaComparator, "A", "B", "C");
        g.addEdge("A", "B");
        g.addEdge("B", "C");
        g.addEdge("C", "A");

        g.topologicalSort();
    }
}