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
package com.datastax.oss.driver.api.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.Collection;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class NodeMetadataIT {

  @ClassRule public static CcmRule ccmRule = CcmRule.getInstance();

  @Rule public SessionRule<CqlSession> sessionRule = new SessionRule<>(ccmRule);

  @Test
  public void should_expose_node_metadata() {
    Collection<Node> values = sessionRule.session().getMetadata().getNodes().values();
    assertThat(values).hasSize(1);

    Node node = values.iterator().next();

    // Run a few basic checks given what we know about our test environment:
    assertThat(node.getConnectAddress()).isNotNull();
    node.getBroadcastAddress()
        .ifPresent(
            broadcastAddress ->
                assertThat(broadcastAddress).isEqualTo(node.getConnectAddress().getAddress()));
    assertThat(node.getListenAddress().get()).isEqualTo(node.getConnectAddress().getAddress());
    assertThat(node.getDatacenter()).isEqualTo("dc1");
    assertThat(node.getRack()).isEqualTo("r1");
    assertThat(node.getCassandraVersion()).isEqualTo(ccmRule.getCassandraVersion());
    assertThat(node.getState()).isSameAs(NodeState.UP);
    assertThat(node.getDistance()).isSameAs(NodeDistance.LOCAL);
    assertThat(node.getHostId()).isNotNull();
    assertThat(node.getSchemaVersion()).isNotNull();

    // Note: open connections and reconnection status are covered in NodeStateIT
  }
}
