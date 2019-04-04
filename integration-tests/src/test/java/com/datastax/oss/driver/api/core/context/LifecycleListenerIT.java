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
package com.datastax.oss.driver.api.core.context;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.api.testinfra.utils.ConditionChecker;
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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class LifecycleListenerIT {

  @ClassRule
  public static SimulacronRule simulacronRule =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @Test
  public void should_notify_listener_of_init_and_shutdown() {
    TestLifecycleListener listener = new TestLifecycleListener();
    assertThat(listener.ready).isFalse();
    assertThat(listener.closed).isFalse();

    try (CqlSession session = newSession(listener)) {
      ConditionChecker.checkThat(() -> listener.ready).before(1, SECONDS).becomesTrue();
      assertThat(listener.closed).isFalse();
    }
    assertThat(listener.ready).isTrue();
    ConditionChecker.checkThat(() -> listener.closed).before(1, SECONDS).becomesTrue();
  }

  @Test
  public void should_not_notify_listener_when_init_fails() {
    TestLifecycleListener listener = new TestLifecycleListener();
    assertThat(listener.ready).isFalse();
    assertThat(listener.closed).isFalse();

    simulacronRule.cluster().rejectConnections(0, RejectScope.STOP);
    try (CqlSession session = newSession(listener)) {
      fail("Expected AllNodesFailedException");
    } catch (AllNodesFailedException ignored) {
    } finally {
      simulacronRule.cluster().acceptConnections();
    }
    assertThat(listener.ready).isFalse();
    ConditionChecker.checkThat(() -> listener.closed).before(1, SECONDS).becomesTrue();
  }

  private CqlSession newSession(TestLifecycleListener listener) {
    TestContext context = new TestContext(new DefaultDriverConfigLoader(), listener);
    return CompletableFutures.getUninterruptibly(
        DefaultSession.init(context, simulacronRule.getContactPoints(), null));
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
