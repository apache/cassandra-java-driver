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
package com.datastax.oss.driver.internal.core.loadbalancing.nodeset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import org.junit.Test;

public class SingleDcNodeSetTest {

  @Test
  public void should_add_node() {
    SingleDcNodeSet set = new SingleDcNodeSet("dc1");
    Node node1 = mockNode("dc1");
    assertThat(set.add(node1)).isTrue();
    assertThat(set.add(node1)).isFalse();
    Node node2 = mockNode("dc2");
    assertThat(set.add(node2)).isFalse();
  }

  @Test
  public void should_remove_node() {
    SingleDcNodeSet set = new SingleDcNodeSet("dc1");
    Node node = mockNode("dc1");
    set.add(node);
    assertThat(set.remove(node)).isTrue();
    assertThat(set.remove(node)).isFalse();
  }

  @Test
  public void should_return_all_nodes_if_local_dc() {
    SingleDcNodeSet set = new SingleDcNodeSet("dc1");
    Node node1 = mockNode("dc1");
    set.add(node1);
    Node node2 = mockNode("dc1");
    set.add(node2);
    Node node3 = mockNode("dc2");
    set.add(node3);
    assertThat(set.dc("dc1")).contains(node1, node2);
    assertThat(set.dc("dc2")).isEmpty();
    assertThat(set.dc(null)).isEmpty();
  }

  @Test
  public void should_return_only_local_dc() {
    SingleDcNodeSet set = new SingleDcNodeSet("dc1");
    assertThat(set.dcs()).contains("dc1");
  }

  private Node mockNode(String dc) {
    Node node = mock(Node.class);
    when(node.getDatacenter()).thenReturn(dc);
    return node;
  }
}
