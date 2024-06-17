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
package com.datastax.oss.driver.core.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.LifecycleListener;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.server.RejectScope;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class LifecycleListenerIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @Test
  public void should_notify_listener_of_init_and_shutdown() {
    TestLifecycleListener listener = new TestLifecycleListener();
    assertThat(listener.ready).isFalse();
    assertThat(listener.closed).isFalse();

    try (CqlSession session = newSession(listener)) {
      await().atMost(1, TimeUnit.SECONDS).until(() -> listener.ready);
      assertThat(listener.closed).isFalse();
    }
    assertThat(listener.ready).isTrue();
    await().atMost(1, TimeUnit.SECONDS).until(() -> listener.closed);
  }

  @Test
  public void should_not_notify_listener_when_init_fails() {
    TestLifecycleListener listener = new TestLifecycleListener();
    assertThat(listener.ready).isFalse();
    assertThat(listener.closed).isFalse();

    SIMULACRON_RULE.cluster().rejectConnections(0, RejectScope.STOP);
    try (CqlSession session = newSession(listener)) {
      fail("Expected AllNodesFailedException");
    } catch (AllNodesFailedException ignored) {
    } finally {
      SIMULACRON_RULE.cluster().acceptConnections();
    }
    assertThat(listener.ready).isFalse();
    await().atMost(1, TimeUnit.SECONDS).until(() -> listener.closed);
  }

  private CqlSession newSession(TestLifecycleListener listener) {
    TestContext context = new TestContext(new DefaultDriverConfigLoader(), listener);
    return CompletableFutures.getUninterruptibly(
        DefaultSession.init(context, SIMULACRON_RULE.getContactPoints(), null));
  }

  public static class TestLifecycleListener implements LifecycleListener {
    volatile boolean ready;
    volatile boolean closed;

    @Override
    public void onSessionReady() {
      ready = true;
    }

    @Override
    public void close() throws Exception {
      closed = true;
    }
  }

  public static class TestContext extends DefaultDriverContext {

    private final List<LifecycleListener> listeners;

    TestContext(DriverConfigLoader configLoader, TestLifecycleListener listener) {
      super(
          configLoader,
          Collections.emptyList(),
          null,
          null,
          null,
          Collections.emptyMap(),
          Collections.emptyMap(),
          null);
      this.listeners = ImmutableList.of(listener);
    }

    @NonNull
    @Override
    public List<LifecycleListener> getLifecycleListeners() {
      return listeners;
    }
  }
}
