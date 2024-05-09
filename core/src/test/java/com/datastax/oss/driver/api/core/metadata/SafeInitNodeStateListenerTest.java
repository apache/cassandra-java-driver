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
package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.session.Session;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SafeInitNodeStateListenerTest {

  @Mock private NodeStateListener delegate;
  @Mock private Node node;
  @Mock private Session session;

  @Test
  public void should_replay_init_events() {
    SafeInitNodeStateListener wrapper = new SafeInitNodeStateListener(delegate, true);

    // Not a realistic sequence of invocations in the driver, but that doesn't matter
    wrapper.onAdd(node);
    wrapper.onUp(node);
    wrapper.onSessionReady(session);
    wrapper.onDown(node);

    InOrder inOrder = Mockito.inOrder(delegate);
    inOrder.verify(delegate).onSessionReady(session);
    inOrder.verify(delegate).onAdd(node);
    inOrder.verify(delegate).onUp(node);
    inOrder.verify(delegate).onDown(node);
  }

  @Test
  public void should_discard_init_events() {
    SafeInitNodeStateListener wrapper = new SafeInitNodeStateListener(delegate, false);

    wrapper.onAdd(node);
    wrapper.onUp(node);
    wrapper.onSessionReady(session);
    wrapper.onDown(node);

    InOrder inOrder = Mockito.inOrder(delegate);
    inOrder.verify(delegate).onSessionReady(session);
    inOrder.verify(delegate).onDown(node);
  }
}
