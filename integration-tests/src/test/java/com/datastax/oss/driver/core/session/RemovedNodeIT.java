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
package com.datastax.oss.driver.core.session;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.utils.ConditionChecker;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.Test;

public class RemovedNodeIT {

  @ClassRule
  public static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withNodes(2).build();

  @Test
  public void should_signal_and_destroy_pool_when_node_gets_removed() {
    RemovalListener removalListener = new RemovalListener();
    try (CqlSession session = CqlSession.builder().withNodeStateListener(removalListener).build()) {
      CCM_RULE.getCcmBridge().nodetool(2, "decommission");
      ConditionChecker.checkThat(() -> removalListener.removedNode != null).becomesTrue();

      Map<Node, ChannelPool> pools = ((DefaultSession) session).getPools();
      ConditionChecker.checkThat(() -> pools.containsKey(removalListener.removedNode))
          .becomesFalse();
    }
  }

  static class RemovalListener implements NodeStateListener {

    volatile Node removedNode;

    @Override
    public void onRemove(@NonNull Node node) {
      removedNode = node;
    }

    @Override
    public void onAdd(@NonNull Node node) {}

    @Override
    public void onUp(@NonNull Node node) {}

    @Override
    public void onDown(@NonNull Node node) {}

    @Override
    public void close() throws Exception {}
  }
}
