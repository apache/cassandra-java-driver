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

public class AddedNodeIT {

  @ClassRule
  public static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withNodes(3).build();

  @Test
  public void should_signal_and_create_pool_when_node_gets_added() {
    AddListener addListener = new AddListener();
    try (CqlSession session = SessionUtils.newSession(CCM_RULE, null, addListener, null, null)) {
      assertThat(session.getMetadata().getTokenMap()).isPresent();
      Set<TokenRange> tokenRanges = session.getMetadata().getTokenMap().get().getTokenRanges();
      assertThat(tokenRanges).hasSize(3);
      CCM_RULE.getCcmBridge().add(4, "dc1");
      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> addListener.addedNode != null);
      Map<Node, ChannelPool> pools = ((DefaultSession) session).getPools();
      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> pools.containsKey(addListener.addedNode));
      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> session.getMetadata().getTokenMap().get().getTokenRanges().size() == 4);
    }
  }

  static class AddListener implements NodeStateListener {

    volatile Node addedNode;

    @Override
    public void onRemove(@NonNull Node node) {}

    @Override
    public void onAdd(@NonNull Node node) {
      addedNode = node;
    }

    @Override
    public void onUp(@NonNull Node node) {}

    @Override
    public void onDown(@NonNull Node node) {}

    @Override
    public void close() throws Exception {}
  }
}
