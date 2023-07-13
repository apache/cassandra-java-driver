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
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.SafeInitNodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@Category(ParallelizableTests.class)
@RunWith(MockitoJUnitRunner.class)
public class ListenersIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @Mock private NodeStateListener nodeListener;
  @Mock private SchemaChangeListener schemaListener;
  @Mock private RequestTracker requestTracker;
  @Captor private ArgumentCaptor<Node> nodeCaptor;

  @Test
  public void should_inject_session_in_listeners() {
    try (CqlSession session =
        (CqlSession)
            SessionUtils.baseBuilder()
                .addContactEndPoints(SIMULACRON_RULE.getContactPoints())
                .withNodeStateListener(new SafeInitNodeStateListener(nodeListener, true))
                .withSchemaChangeListener(schemaListener)
                .withRequestTracker(requestTracker)
                .build()) {

      InOrder inOrder = inOrder(nodeListener);
      inOrder.verify(nodeListener).onSessionReady(session);
      inOrder.verify(nodeListener).onUp(nodeCaptor.capture());
      assertThat(nodeCaptor.getValue().getEndPoint())
          .isEqualTo(SIMULACRON_RULE.getContactPoints().iterator().next());

      verify(schemaListener).onSessionReady(session);
      verify(requestTracker).onSessionReady(session);
    }
  }
}
