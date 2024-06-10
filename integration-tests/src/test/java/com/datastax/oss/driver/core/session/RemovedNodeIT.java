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
package com.datastax.oss.driver.core.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.testinfra.ScyllaSkip;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Test;

@ScyllaSkip(description = "@IntegrationTestDisabledFlaky")
public class RemovedNodeIT {

  @ClassRule
  public static final CustomCcmRule CCM_RULE =
      CustomCcmRule.builder()
          // We need 4 nodes to run this test against DSE, because it requires at least 3 nodes to
          // maintain RF=3 for keyspace system_distributed
          .withNodes(4)
          .build();

  @Test
  public void should_signal_and_destroy_pool_when_node_gets_removed() {
    RemovalListener removalListener = new RemovalListener();
    try (CqlSession session =
        SessionUtils.newSession(CCM_RULE, null, removalListener, null, null)) {
      assertThat(session.getMetadata().getTokenMap()).isPresent();
      Set<TokenRange> tokenRanges = session.getMetadata().getTokenMap().get().getTokenRanges();
      assertThat(tokenRanges).hasSize(4);
      CCM_RULE.getCcmBridge().decommission(2);
      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> removalListener.removedNode != null);
      Map<Node, ChannelPool> pools = ((DefaultSession) session).getPools();
      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> !pools.containsKey(removalListener.removedNode));
      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> session.getMetadata().getTokenMap().get().getTokenRanges().size() == 3);
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
