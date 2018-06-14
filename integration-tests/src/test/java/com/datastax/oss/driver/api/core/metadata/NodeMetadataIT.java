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
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.utils.ConditionChecker;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import java.util.Collection;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class NodeMetadataIT {

  @ClassRule public static CcmRule ccmRule = CcmRule.getInstance();

  @Rule public SessionRule<CqlSession> sessionRule = SessionRule.builder(ccmRule).build();

  @Test
  public void should_expose_node_metadata() {
    CqlSession session = sessionRule.session();
    Node node = getUniqueNode(session);

    // Run a few basic checks given what we know about our test environment:
    assertThat(node.getConnectAddress()).isNotNull();
    node.getBroadcastAddress()
        .ifPresent(
            broadcastAddress ->
                assertThat(broadcastAddress.getAddress())
                    .isEqualTo(node.getConnectAddress().getAddress()));
    assertThat(node.getListenAddress().get().getAddress())
        .isEqualTo(node.getConnectAddress().getAddress());
    assertThat(node.getDatacenter()).isEqualTo("dc1");
    assertThat(node.getRack()).isEqualTo("r1");
    assertThat(node.getCassandraVersion()).isEqualTo(ccmRule.getCassandraVersion());
    assertThat(node.getState()).isSameAs(NodeState.UP);
    assertThat(node.getDistance()).isSameAs(NodeDistance.LOCAL);
    assertThat(node.getHostId()).isNotNull();
    assertThat(node.getSchemaVersion()).isNotNull();
    long upTime1 = node.getUpSinceMillis();
    assertThat(upTime1).isGreaterThan(-1);
    assertThat(node.getLastResponseTimeNanos()).isEqualTo(-1);

    // Note: open connections and reconnection status are covered in NodeStateIT

    // Force the node down and back up to check that upSinceMillis gets updated
    EventBus eventBus = ((InternalDriverContext) session.getContext()).eventBus();
    eventBus.fire(TopologyEvent.forceDown(node.getConnectAddress()));
    ConditionChecker.checkThat(() -> node.getState() == NodeState.FORCED_DOWN).becomesTrue();
    assertThat(node.getUpSinceMillis()).isEqualTo(-1);
    eventBus.fire(TopologyEvent.forceUp(node.getConnectAddress()));
    ConditionChecker.checkThat(() -> node.getState() == NodeState.UP).becomesTrue();
    assertThat(node.getUpSinceMillis()).isGreaterThan(upTime1);
  }

  @Test
  public void should_not_record_last_response_time_if_disabled() {
    CqlSession session = sessionRule.session();
    Node node = getUniqueNode(session);
    assertThat(node.getLastResponseTimeNanos()).isEqualTo(-1);

    for (int i = 0; i < 10; i++) {
      session.execute("SELECT release_version FROM system.local");
      assertThat(node.getLastResponseTimeNanos()).isEqualTo(-1);
    }
  }

  @Test
  public void should_record_last_response_time_if_enabled() {
    try (CqlSession session =
        SessionUtils.newSession(
            ccmRule,
            SessionUtils.configLoaderBuilder()
                .withBoolean(DefaultDriverOption.METADATA_LAST_RESPONSE_TIME_ENABLED, true)
                .build())) {
      Node node = getUniqueNode(session);

      // Ensure that we get increasing timestamps as long as we keep querying
      long[] timestamps = new long[10];
      timestamps[0] = System.nanoTime();
      for (int i = 1; i < 9; i++) {
        session.execute("SELECT release_version FROM system.local");
        timestamps[i] = node.getLastResponseTimeNanos();
      }
      timestamps[9] = System.nanoTime();

      for (int i = 0; i < 9; i++) {
        assertThat(timestamps[i]).isLessThan(timestamps[i + 1]);
      }

      // Ensure that the value doesn't change since the last query
      assertThat(node.getLastResponseTimeNanos()).isEqualTo(timestamps[8]);
    }
  }

  private static Node getUniqueNode(CqlSession session) {
    Collection<Node> nodes = session.getMetadata().getNodes().values();
    assertThat(nodes).hasSize(1);
    return nodes.iterator().next();
  }
}
