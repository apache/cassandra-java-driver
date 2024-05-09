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

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class ShutdownIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  private static final String QUERY_STRING = "select * from foo";

  @Test
  public void should_fail_requests_when_session_is_closed() throws Exception {
    // Given
    // Prime with a bit of delay to increase the chance that a query will be aborted in flight when
    // we force-close the session
    SIMULACRON_RULE
        .cluster()
        .prime(when(QUERY_STRING).then(noRows()).delay(20, TimeUnit.MILLISECONDS));
    CqlSession session = SessionUtils.newSession(SIMULACRON_RULE);

    // When
    // Max out the in-flight requests on the connection (from a separate thread pool to get a bit of
    // contention), then force-close the session abruptly.
    Set<String> unexpectedErrors = new ConcurrentSkipListSet<>();
    ExecutorService requestExecutor = Executors.newFixedThreadPool(4);
    int maxConcurrentRequests =
        session
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .getInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS);
    Semaphore semaphore = new Semaphore(maxConcurrentRequests);
    CountDownLatch gotSessionClosedError = new CountDownLatch(1);
    for (int i = 0; i < 4; i++) {
      requestExecutor.execute(
          () -> {
            try {
              while (!Thread.currentThread().isInterrupted()) {
                semaphore.acquire();
                session
                    .executeAsync(QUERY_STRING)
                    .whenComplete(
                        (ignoredResult, error) -> {
                          semaphore.release();
                          // Four things can happen:
                          // - DefaultSession.execute() detects that it's closed and fails the
                          //   request immediately
                          // - the request was in flight and gets aborted when its channel is
                          //   force-closed => ClosedConnectionException
                          // - the request races with the shutdown: it gets past execute() but by
                          //   the time it tries to acquire a channel the pool was closed
                          //   => NoNodeAvailableException
                          // - the request races with the channel closing: it acquires a channel,
                          //   but by the time it tries to write on it is closing
                          //   => AllNodesFailedException wrapping IllegalStateException
                          if (error instanceof IllegalStateException
                              && "Session is closed".equals(error.getMessage())) {
                            gotSessionClosedError.countDown();
                          } else if (error instanceof AllNodesFailedException) {
                            AllNodesFailedException anfe = (AllNodesFailedException) error;
                            // if there were 0 errors, its a NoNodeAvailableException which is
                            // acceptable.
                            if (anfe.getAllErrors().size() > 0) {
                              assertThat(anfe.getAllErrors()).hasSize(1);
                              error = anfe.getAllErrors().values().iterator().next().get(0);
                              if (!(error instanceof IllegalStateException)
                                  && !error.getMessage().endsWith("is closing")) {
                                unexpectedErrors.add(error.toString());
                              }
                            }
                          } else if (error != null
                              && !(error instanceof ClosedConnectionException)) {
                            unexpectedErrors.add(error.toString());
                          }
                        });
              }
            } catch (InterruptedException e) {
              // return
            }
          });
    }
    TimeUnit.MILLISECONDS.sleep(1000);
    session.forceCloseAsync();
    assertThat(gotSessionClosedError.await(10, TimeUnit.SECONDS))
        .as("Expected to get the 'Session is closed' error shortly after shutting down")
        .isTrue();
    requestExecutor.shutdownNow();

    // Then
    assertThat(unexpectedErrors).isEmpty();
  }

  @Test
  public void should_handle_getting_closed_twice() {
    CqlSession session = SessionUtils.newSession(SIMULACRON_RULE);
    session.close();
    session.close();
  }
}
