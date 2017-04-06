/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.util.concurrent;

import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy.ReconnectionSchedule;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.EventExecutor;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;

public class ReconnectionTest {

  @Mock private ReconnectionPolicy reconnectionPolicy;
  @Mock private ReconnectionSchedule reconnectionSchedule;
  private EmbeddedChannel channel;

  private MockReconnectionTask reconnectionTask;
  private Reconnection reconnection;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);

    Mockito.when(reconnectionPolicy.newSchedule()).thenReturn(reconnectionSchedule);

    // Unfortunately Netty does not expose EmbeddedEventLoop, so we have to go through a channel
    channel = new EmbeddedChannel();
    EventExecutor eventExecutor = channel.eventLoop();

    reconnectionTask = new MockReconnectionTask();
    reconnection = new Reconnection(eventExecutor, reconnectionPolicy, reconnectionTask);
  }

  @Test
  public void should_start_out_not_running() {
    assertThat(reconnection.isRunning()).isFalse();
  }

  @Test
  public void should_schedule_first_attempt_on_start() {
    // Given
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofSeconds(1));

    // When
    reconnection.start();

    // Then
    Mockito.verify(reconnectionSchedule).nextDelay();
    assertThat(reconnection.isRunning()).isTrue();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void should_fail_if_started_twice() {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofSeconds(1));
    reconnection.start();
    reconnection.start();
  }

  @Test
  public void should_stop_if_first_attempt_succeeds() {
    // Given
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    reconnection.start();
    Mockito.verify(reconnectionSchedule).nextDelay();

    // When
    // the reconnection task is scheduled:
    runPendingTasks();
    assertThat(reconnectionTask.wasCalled()).isTrue();
    // the reconnection task completes:
    reconnectionTask.complete(true);
    runPendingTasks();

    // Then
    assertThat(reconnection.isRunning()).isFalse();
  }

  @Test
  public void should_reschedule_if_first_attempt_fails() {
    // Given
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    reconnection.start();
    Mockito.verify(reconnectionSchedule).nextDelay();

    // When
    // the reconnection task is scheduled:
    runPendingTasks();
    assertThat(reconnectionTask.wasCalled()).isTrue();
    // the reconnection task completes:
    reconnectionTask.complete(false);
    runPendingTasks();

    // Then
    // schedule was called again
    Mockito.verify(reconnectionSchedule, times(2)).nextDelay();
    runPendingTasks();
    // task was called again
    assertThat(reconnectionTask.wasCalled()).isTrue();
    // still running
    assertThat(reconnection.isRunning()).isTrue();

    // When
    // second attempt completes
    reconnectionTask.complete(true);
    runPendingTasks();

    // Then
    assertThat(reconnection.isRunning()).isFalse();
  }

  @Test
  public void should_stop_between_attempts_if_requested() {
    // Given
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofSeconds(10));
    reconnection.start();
    Mockito.verify(reconnectionSchedule).nextDelay();

    // When
    reconnection.stop();

    // Then
    assertThat(reconnection.isRunning()).isFalse();
  }

  private void runPendingTasks() {
    channel.runPendingTasks();
  }

  private static class MockReconnectionTask implements Callable<CompletionStage<Boolean>> {
    private volatile CompletableFuture<Boolean> nextResult;

    @Override
    public CompletionStage<Boolean> call() throws Exception {
      assertThat(nextResult == null || nextResult.isDone()).isTrue();
      nextResult = new CompletableFuture<>();
      return nextResult;
    }

    private void complete(boolean outcome) {
      assertThat(nextResult != null || !nextResult.isDone()).isTrue();
      nextResult.complete(outcome);
      nextResult = null;
    }

    private boolean wasCalled() {
      return nextResult != null;
    }
  }
}
