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
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public abstract class QueryPlanTestBase {

  @Mock private Node node1;
  @Mock private Node node2;
  @Mock private Node node3;

  @Test
  public void should_poll_elements() {
    QueryPlan queryPlan = newQueryPlan(node1, node2, node3);
    assertThat(queryPlan.poll()).isSameAs(node1);
    assertThat(queryPlan.poll()).isSameAs(node2);
    assertThat(queryPlan.poll()).isSameAs(node3);
    assertThat(queryPlan.poll()).isNull();
    assertThat(queryPlan.poll()).isNull();
  }

  @Test
  public void should_poll_elements_concurrently() throws InterruptedException {
    for (int runs = 0; runs < 5; runs++) {
      Node[] nodes = new Node[1000];
      for (int i = 0; i < 1000; i++) {
        nodes[i] = mock(Node.class, "node" + i);
        when(nodes[i].getOpenConnections()).thenReturn(i);
      }
      QueryPlan queryPlan = newQueryPlan(nodes);
      Set<Node> actual =
          new ConcurrentSkipListSet<>(Comparator.comparingInt(Node::getOpenConnections));
      Thread[] threads = new Thread[5];
      for (int i = 0; i < 5; i++) {
        threads[i] =
            new Thread(
                () -> {
                  while (true) {
                    Node node = queryPlan.poll();
                    if (node == null) {
                      return;
                    }
                    actual.add(node);
                  }
                });
      }
      for (Thread thread : threads) {
        thread.start();
      }
      for (Thread thread : threads) {
        thread.join();
      }
      assertThat(actual).hasSize(1000);
      Iterator<Node> iterator = actual.iterator();
      for (int i = 0; iterator.hasNext(); i++) {
        Node node = iterator.next();
        assertThat(node.getOpenConnections()).isEqualTo(i);
      }
    }
  }

  @Test
  public void should_return_size() {
    QueryPlan queryPlan = newQueryPlan(node1, node2, node3);
    assertThat(queryPlan.size()).isEqualTo(3);
    queryPlan.poll();
    assertThat(queryPlan.size()).isEqualTo(2);
    queryPlan.poll();
    assertThat(queryPlan.size()).isEqualTo(1);
    queryPlan.poll();
    assertThat(queryPlan.size()).isEqualTo(0);
    queryPlan.poll();
    assertThat(queryPlan.size()).isEqualTo(0);
  }

  @Test
  public void should_return_iterator() {
    QueryPlan queryPlan = newQueryPlan(node1, node2, node3);
    Iterator<Node> iterator3 = queryPlan.iterator();
    queryPlan.poll();
    Iterator<Node> iterator2 = queryPlan.iterator();
    queryPlan.poll();
    Iterator<Node> iterator1 = queryPlan.iterator();
    queryPlan.poll();
    Iterator<Node> iterator0 = queryPlan.iterator();
    queryPlan.poll();
    Iterator<Node> iterator00 = queryPlan.iterator();

    assertThat(iterator3).toIterable().containsExactly(node1, node2, node3);
    assertThat(iterator2).toIterable().containsExactly(node2, node3);
    assertThat(iterator1).toIterable().containsExactly(node3);
    assertThat(iterator0).toIterable().isEmpty();
    assertThat(iterator00).toIterable().isEmpty();
  }

  protected abstract QueryPlan newQueryPlan(Node... nodes);
}
