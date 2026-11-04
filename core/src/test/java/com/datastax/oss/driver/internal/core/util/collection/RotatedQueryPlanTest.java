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
package com.datastax.oss.driver.internal.core.util.collection;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RotatedQueryPlanTest extends QueryPlanTestBase {

  @Override
  protected QueryPlan newQueryPlan(Node... nodes) {
    return new RotatedQueryPlan(nodes, 0, 0);
  }

  @Test
  public void should_rotate_nodes_from_start_index() {
    Node[] nodes = newNodes(5);

    assertThat(new RotatedQueryPlan(nodes, 1, 1))
        .containsExactly(nodes[0], nodes[2], nodes[3], nodes[4], nodes[1]);
    assertThat(new RotatedQueryPlan(nodes, 0, 2))
        .containsExactly(nodes[2], nodes[3], nodes[4], nodes[0], nodes[1]);
    assertThat(new RotatedQueryPlan(nodes, 2, 10))
        .containsExactly(nodes[0], nodes[1], nodes[3], nodes[4], nodes[2]);
  }

  @Test
  public void should_not_rotate_when_amount_is_zero_negative_or_multiple_of_range_size() {
    Node[] nodes = newNodes(5);

    assertThat(new RotatedQueryPlan(nodes, 1, 0)).containsExactly(nodes);
    assertThat(new RotatedQueryPlan(nodes, 1, -1)).containsExactly(nodes);
    assertThat(new RotatedQueryPlan(nodes, 1, 8)).containsExactly(nodes);
  }

  @Test
  public void should_not_rotate_when_range_is_singleton_or_empty() {
    Node[] nodes = newNodes(5);

    assertThat(new RotatedQueryPlan(nodes, 4, 3)).containsExactly(nodes);
    assertThat(new RotatedQueryPlan(nodes, 5, 3)).containsExactly(nodes);
  }

  /**
   * This test ensures that {@link RotatedQueryPlan} has the same order as the previous rotation
   * implementation that lived in {@link ArrayUtils}.
   */
  @Test
  public void should_poll_and_iterate_like_previous_array_rotation() {
    Node[] source = newNodes(8);
    int[] amounts = {
      Integer.MIN_VALUE, -1, 0, 1, 2, 3, 7, 8, 9, 17, Integer.MAX_VALUE - 1, Integer.MAX_VALUE
    };
    for (int length = 0; length <= source.length; length++) {
      for (int startIndex = 0; startIndex <= length; startIndex++) {
        for (int amount : amounts) {
          Object[] expected = new Object[length];
          System.arraycopy(source, 0, expected, 0, length);
          previousRotate(expected, startIndex, length - startIndex, amount);
          Object[] nodes = new Object[length];
          System.arraycopy(source, 0, nodes, 0, length);

          for (int polled = 0; polled <= length; polled++) {
            QueryPlan expectedPlan = new SimpleQueryPlan(expected);
            QueryPlan actualPlan = new RotatedQueryPlan(nodes, startIndex, amount);
            for (int i = 0; i < polled; i++) {
              assertThat(actualPlan.poll()).isSameAs(expectedPlan.poll());
            }
            assertThat(actualPlan.size()).isEqualTo(expectedPlan.size());
            assertThat(actualPlan).containsExactlyElementsOf(expectedPlan);
            assertThat(drain(actualPlan)).containsExactlyElementsOf(drain(expectedPlan));
          }
        }
      }
    }
  }

  private static Node[] newNodes(int count) {
    Node[] nodes = new Node[count];
    for (int i = 0; i < count; i++) {
      nodes[i] = mock(Node.class, "node" + i);
    }
    return nodes;
  }

  /**
   * The array rotation that load balancing policies applied before they used {@link
   * RotatedQueryPlan}: formerly {@code ArrayUtils.rotate}, copied verbatim with its {@code
   * bubbleDown} inlined.
   */
  private static void previousRotate(Object[] elements, int startIndex, int length, int amount) {
    if (length >= 2) {
      amount = amount % length;
      for (int i = 0; i < amount; i++) {
        for (int j = startIndex; j < startIndex + length - 1; j++) {
          ArrayUtils.swap(elements, j, j + 1);
        }
      }
    }
  }

  private static List<Node> drain(QueryPlan plan) {
    List<Node> list = new ArrayList<>();
    Node node;
    while ((node = plan.poll()) != null) {
      list.add(node);
    }
    return list;
  }
}
