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

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.session.SessionLifecycleManager;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.PoolManager;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class SuspendIT {
  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(2));

  private static final String QUERY_STRING = "select * from foo";

  @Test
  public void should_resume_after_suspend() throws Exception {
    SIMULACRON_RULE.cluster().prime(when(QUERY_STRING).then(noRows()));

    CqlSession session = SessionUtils.newSession(SIMULACRON_RULE);
    assertThat(session.execute(QUERY_STRING).all().size()).isEqualTo(0);

    SessionLifecycleManager manager = SessionLifecycleManager.of(session);
    manager.suspend();

    PoolManager poolManager = ((InternalDriverContext) session.getContext()).getPoolManager();
    assertThat(poolManager.getPools().size()).isEqualTo(0);
    assertThatThrownBy(() -> session.execute(QUERY_STRING).all())
        .isInstanceOf(NoNodeAvailableException.class);

    manager.resume();

    // Busy waiting - PoolManager does not expose any listeners on added node.
    // After ChannelEvent.Type.OPEN the future is added to PoolManager.SingleThreaded.pending
    // but this map is not exposed (and not synchronized)
    await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(10, TimeUnit.MILLISECONDS)
        .until(() -> poolManager.getPools().size() > 0);

    assertThat(session.execute(QUERY_STRING).all().size()).isEqualTo(0);
  }
}
