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

import com.datastax.oss.driver.api.core.metadata.Node;
import org.junit.Test;

public class DcAgnosticNodeSetTest {

  @Test
  public void should_add_node() {
    DcAgnosticNodeSet set = new DcAgnosticNodeSet();
    Node node = mock(Node.class);
    assertThat(set.add(node)).isTrue();
    assertThat(set.add(node)).isFalse();
  }

  @Test
  public void should_remove_node() {
    DcAgnosticNodeSet set = new DcAgnosticNodeSet();
    Node node = mock(Node.class);
    set.add(node);
    assertThat(set.remove(node)).isTrue();
    assertThat(set.remove(node)).isFalse();
  }

  @Test
  public void should_return_all_nodes() {
    DcAgnosticNodeSet set = new DcAgnosticNodeSet();
    Node node1 = mock(Node.class);
    set.add(node1);
    Node node2 = mock(Node.class);
    set.add(node2);
    assertThat(set.dc(null)).contains(node1, node2);
    assertThat(set.dc("irrelevant")).contains(node1, node2);
  }

  @Test
  public void should_return_empty_dcs() {
    DcAgnosticNodeSet set = new DcAgnosticNodeSet();
    assertThat(set.dcs()).isEmpty();
  }
}
