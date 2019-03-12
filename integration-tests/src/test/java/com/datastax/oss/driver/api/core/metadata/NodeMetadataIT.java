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
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.utils.ConditionChecker;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import java.net.InetSocketAddress;
import java.util.Collection;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class NodeMetadataIT {

  @ClassRule public static CcmRule ccmRule = CcmRule.getInstance();

  @Test
  public void should_expose_node_metadata() {
    try (CqlSession session = SessionUtils.newSession(ccmRule)) {
      Node node = getUniqueNode(session);
      // Run a few basic checks given what we know about our test environment:
      assertThat(node.getEndPoint()).isNotNull();
      InetSocketAddress connectAddress = (InetSocketAddress) node.getEndPoint().resolve();
      node.getBroadcastAddress()
          .ifPresent(
              broadcastAddress ->
                  assertThat(broadcastAddress.getAddress()).isEqualTo(connectAddress.getAddress()));
      assertThat(node.getListenAddress().get().getAddress()).isEqualTo(connectAddress.getAddress());
      assertThat(node.getDatacenter()).isEqualTo("dc1");
      assertThat(node.getRack()).isEqualTo("r1");
      if (!CcmBridge.DSE_ENABLEMENT) {
        // CcmBridge does not report accurate C* versions for DSE, only approximated values
        assertThat(node.getCassandraVersion()).isEqualTo(ccmRule.getCassandraVersion());
      }
      assertThat(node.getState()).isSameAs(NodeState.UP);
      assertThat(node.getDistance()).isSameAs(NodeDistance.LOCAL);
      assertThat(node.getHostId()).isNotNull();
      assertThat(node.getSchemaVersion()).isNotNull();
      long upTime1 = node.getUpSinceMillis();
      assertThat(upTime1).isGreaterThan(-1);

      // Note: open connections and reconnection status are covered in NodeStateIT

      // Force the node down and back up to check that upSinceMillis gets updated
      EventBus eventBus = ((InternalDriverContext) session.getContext()).getEventBus();
      eventBus.fire(TopologyEvent.forceDown(node.getBroadcastRpcAddress().get()));
      ConditionChecker.checkThat(() -> node.getState() == NodeState.FORCED_DOWN).becomesTrue();
      assertThat(node.getUpSinceMillis()).isEqualTo(-1);
      eventBus.fire(TopologyEvent.forceUp(node.getBroadcastRpcAddress().get()));
      ConditionChecker.checkThat(() -> node.getState() == NodeState.UP).becomesTrue();
      assertThat(node.getUpSinceMillis()).isGreaterThan(upTime1);
    }
  }

  private static Node getUniqueNode(CqlSession session) {
    Collection<Node> nodes = session.getMetadata().getNodes().values();
    assertThat(nodes).hasSize(1);
    return nodes.iterator().next();
  }
}
