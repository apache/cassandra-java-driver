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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.NodeStateListenerBase;
import com.datastax.oss.driver.api.core.metadata.SafeInitNodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListenerBase;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
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

  @Mock private NodeStateListener nodeListener1;
  @Mock private NodeStateListener nodeListener2;
  @Mock private SchemaChangeListener schemaListener1;
  @Mock private SchemaChangeListener schemaListener2;
  @Mock private RequestTracker requestTracker1;
  @Mock private RequestTracker requestTracker2;

  @Captor private ArgumentCaptor<Node> nodeCaptor1;
  @Captor private ArgumentCaptor<Node> nodeCaptor2;

  @Test
  public void should_inject_session_in_listeners() throws Exception {
    try (CqlSession session =
        (CqlSession)
            SessionUtils.baseBuilder()
                .addContactEndPoints(SIMULACRON_RULE.getContactPoints())
                .addNodeStateListener(new SafeInitNodeStateListener(nodeListener1, true))
                .addNodeStateListener(new SafeInitNodeStateListener(nodeListener2, true))
                .addSchemaChangeListener(schemaListener1)
                .addSchemaChangeListener(schemaListener2)
                .addRequestTracker(requestTracker1)
                .addRequestTracker(requestTracker2)
                .withConfigLoader(
                    SessionUtils.configLoaderBuilder()
                        .withClassList(
                            DefaultDriverOption.METADATA_NODE_STATE_LISTENER_CLASSES,
                            Collections.singletonList(MyNodeStateListener.class))
                        .withClassList(
                            DefaultDriverOption.METADATA_SCHEMA_CHANGE_LISTENER_CLASSES,
                            Collections.singletonList(MySchemaChangeListener.class))
                        .withClassList(
                            DefaultDriverOption.REQUEST_TRACKER_CLASSES,
                            Collections.singletonList(MyRequestTracker.class))
                        .build())
                .build()) {

      InOrder inOrder1 = inOrder(nodeListener1);
      inOrder1.verify(nodeListener1).onSessionReady(session);
      inOrder1.verify(nodeListener1).onUp(nodeCaptor1.capture());

      InOrder inOrder2 = inOrder(nodeListener2);
      inOrder2.verify(nodeListener2).onSessionReady(session);
      inOrder2.verify(nodeListener2).onUp(nodeCaptor2.capture());

      assertThat(nodeCaptor1.getValue().getEndPoint())
          .isEqualTo(SIMULACRON_RULE.getContactPoints().iterator().next());

      assertThat(nodeCaptor2.getValue().getEndPoint())
          .isEqualTo(SIMULACRON_RULE.getContactPoints().iterator().next());

      verify(schemaListener1).onSessionReady(session);
      verify(schemaListener2).onSessionReady(session);

      verify(requestTracker1).onSessionReady(session);
      verify(requestTracker2).onSessionReady(session);

      assertThat(MyNodeStateListener.onSessionReadyCalled).isTrue();
      assertThat(MyNodeStateListener.onUpCalled).isTrue();

      assertThat(MySchemaChangeListener.onSessionReadyCalled).isTrue();

      assertThat(MyRequestTracker.onSessionReadyCalled).isTrue();
    }

    verify(nodeListener1).close();
    verify(nodeListener2).close();

    verify(schemaListener1).close();
    verify(schemaListener2).close();

    verify(requestTracker1).close();
    verify(requestTracker2).close();

    assertThat(MyNodeStateListener.closeCalled).isTrue();
    assertThat(MySchemaChangeListener.closeCalled).isTrue();
    assertThat(MyRequestTracker.closeCalled).isTrue();
  }

  public static class MyNodeStateListener extends SafeInitNodeStateListener {

    private static volatile boolean onSessionReadyCalled = false;
    private static volatile boolean onUpCalled = false;
    private static volatile boolean closeCalled = false;

    public MyNodeStateListener(@SuppressWarnings("unused") DriverContext ignored) {
      super(
          new NodeStateListenerBase() {

            @Override
            public void onSessionReady(@NonNull Session session) {
              onSessionReadyCalled = true;
            }

            @Override
            public void onUp(@NonNull Node node) {
              onUpCalled = true;
            }

            @Override
            public void close() {
              closeCalled = true;
            }
          },
          true);
    }
  }

  public static class MySchemaChangeListener extends SchemaChangeListenerBase {

    private static volatile boolean onSessionReadyCalled = false;
    private static volatile boolean closeCalled = false;

    public MySchemaChangeListener(@SuppressWarnings("unused") DriverContext ignored) {}

    @Override
    public void onSessionReady(@NonNull Session session) {
      onSessionReadyCalled = true;
    }

    @Override
    public void close() throws Exception {
      closeCalled = true;
    }
  }

  public static class MyRequestTracker implements RequestTracker {

    private static volatile boolean onSessionReadyCalled = false;
    private static volatile boolean closeCalled = false;

    public MyRequestTracker(@SuppressWarnings("unused") DriverContext ignored) {}

    @Override
    public void onSessionReady(@NonNull Session session) {
      onSessionReadyCalled = true;
    }

    @Override
    public void close() throws Exception {
      closeCalled = true;
    }
  }
}
