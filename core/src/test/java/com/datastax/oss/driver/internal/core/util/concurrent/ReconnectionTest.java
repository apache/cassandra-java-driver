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
package com.datastax.oss.driver.internal.core.util.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.TestDataProviders;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy.ReconnectionSchedule;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.EventExecutor;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(DataProviderRunner.class)
public class ReconnectionTest {

  @Mock private ReconnectionSchedule reconnectionSchedule;
  @Mock private Runnable onStartCallback;
  @Mock private Runnable onStopCallback;
  private EmbeddedChannel channel;

  private MockReconnectionTask reconnectionTask;
  private Reconnection reconnection;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    // Unfortunately Netty does not expose EmbeddedEventLoop, so we have to go through a channel
    channel = new EmbeddedChannel();
    EventExecutor eventExecutor = channel.eventLoop();

    reconnectionTask = new MockReconnectionTask();
    reconnection =
        new Reconnection(
            "test",
            eventExecutor,
            () -> reconnectionSchedule,
            reconnectionTask,
            onStartCallback,
            onStopCallback);
  }

  @Test
  public void should_start_out_not_running() {
    assertThat(reconnection.isRunning()).isFalse();
  }

  @Test
  public void should_schedule_first_attempt_on_start() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofSeconds(1));

    // When
    reconnection.start();

    // Then
    verify(reconnectionSchedule).nextDelay();
    assertThat(reconnection.isRunning()).isTrue();
    verify(onStartCallback).run();
  }

  @Test
  public void should_ignore_start_if_already_started() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofSeconds(1));
    reconnection.start();
    verify(reconnectionSchedule).nextDelay();
    verify(onStartCallback).run();

    // When
    reconnection.start();

    // Then
    verifyNoMoreInteractions(reconnectionSchedule, onStartCallback);
  }

  @Test
  public void should_stop_if_first_attempt_succeeds() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    reconnection.start();
    verify(reconnectionSchedule).nextDelay();

    // When
    // the reconnection task is scheduled:
    runPendingTasks();
    assertThat(reconnectionTask.callCount()).isEqualTo(1);
    // the reconnection task completes:
    reconnectionTask.complete(true);
    runPendingTasks();

    // Then
    assertThat(reconnection.isRunning()).isFalse();
    verify(onStopCallback).run();
  }

  @Test
  public void should_reschedule_if_first_attempt_fails() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    reconnection.start();
    verify(reconnectionSchedule).nextDelay();

    // When
    // the reconnection task is scheduled:
    runPendingTasks();
    assertThat(reconnectionTask.callCount()).isEqualTo(1);
    // the reconnection task completes:
    reconnectionTask.complete(false);
    runPendingTasks();

    // Then
    // schedule was called again
    verify(reconnectionSchedule, times(2)).nextDelay();
    runPendingTasks();
    // task was called again
    assertThat(reconnectionTask.callCount()).isEqualTo(2);
    // still running
    assertThat(reconnection.isRunning()).isTrue();

    // When
    // second attempt completes
    reconnectionTask.complete(true);
    runPendingTasks();

    // Then
    assertThat(reconnection.isRunning()).isFalse();
    verify(onStopCallback).run();
  }

  @Test
  public void should_reconnect_now_if_next_attempt_not_started() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofDays(1));
    reconnection.start();
    verify(reconnectionSchedule).nextDelay();

    // When
    reconnection.reconnectNow(false);
    runPendingTasks();

    // Then
    // reconnection task was run immediately
    assertThat(reconnectionTask.callCount()).isEqualTo(1);
    // if that attempt fails, another reconnection should be scheduled
    reconnectionTask.complete(false);
    runPendingTasks();
    verify(reconnectionSchedule, times(2)).nextDelay();
  }

  @Test
  public void should_reconnect_now_if_stopped_and_forced() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofDays(1));
    assertThat(reconnection.isRunning()).isFalse();

    // When
    reconnection.reconnectNow(true);
    runPendingTasks();

    // Then
    // reconnection task was run immediately
    assertThat(reconnectionTask.callCount()).isEqualTo(1);
    // if that attempt failed, another reconnection was scheduled
    reconnectionTask.complete(false);
    runPendingTasks();
    verify(reconnectionSchedule).nextDelay();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "booleans")
  public void should_reconnect_now_when_attempt_in_progress(boolean force) {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    reconnection.start();
    runPendingTasks();
    // the next scheduled attempt has started, but not completed yet
    assertThat(reconnectionTask.callCount()).isEqualTo(1);

    // When
    reconnection.reconnectNow(force);
    runPendingTasks();

    // Then
    // reconnection task should not have been called again
    assertThat(reconnectionTask.callCount()).isEqualTo(1);
    // should still run until current attempt completes
    assertThat(reconnection.isRunning()).isTrue();
    reconnectionTask.complete(true);
    runPendingTasks();
    assertThat(reconnection.isRunning()).isFalse();
  }

  @Test
  public void should_not_reconnect_now_if_stopped_and_not_forced() {
    // Given
    assertThat(reconnection.isRunning()).isFalse();

    // When
    reconnection.reconnectNow(false);
    runPendingTasks();

    // Then
    assertThat(reconnectionTask.callCount()).isEqualTo(0);
  }

  @Test
  public void should_stop_between_attempts() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofSeconds(10));
    reconnection.start();
    runPendingTasks();
    verify(reconnectionSchedule).nextDelay();

    // When
    reconnection.stop();
    runPendingTasks();

    // Then
    verify(onStopCallback).run();
    assertThat(reconnection.isRunning()).isFalse();
  }

  @Test
  public void should_restart_after_stopped_between_attempts() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofSeconds(10));
    reconnection.start();
    runPendingTasks();
    verify(reconnectionSchedule).nextDelay();
    reconnection.stop();
    runPendingTasks();
    assertThat(reconnection.isRunning()).isFalse();

    // When
    reconnection.start();
    runPendingTasks();

    // Then
    verify(reconnectionSchedule, times(2)).nextDelay();
    assertThat(reconnection.isRunning()).isTrue();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "booleans")
  public void should_stop_while_attempt_in_progress(boolean outcome) {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    reconnection.start();
    runPendingTasks();
    // the next scheduled attempt has started, but not completed yet
    assertThat(reconnectionTask.callCount()).isEqualTo(1);
    verify(onStartCallback).run();

    // When
    reconnection.stop();
    runPendingTasks();

    // Then
    // should let the current attempt complete (whatever its outcome), and become stopped only then
    assertThat(reconnection.isRunning()).isTrue();
    verifyNoMoreInteractions(onStopCallback);
    reconnectionTask.complete(outcome);
    runPendingTasks();
    verify(onStopCallback).run();
    assertThat(reconnection.isRunning()).isFalse();
  }

  @Test
  public void should_restart_after_stopped_while_attempt_in_progress() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    reconnection.start();
    runPendingTasks();
    // the next scheduled attempt has started, but not completed yet
    assertThat(reconnectionTask.callCount()).isEqualTo(1);
    verify(onStartCallback).run();
    // now stop
    reconnection.stop();
    runPendingTasks();
    assertThat(reconnection.isRunning()).isTrue();

    // When
    reconnection.start();
    runPendingTasks();

    // Then
    assertThat(reconnection.isRunning()).isTrue();
    // still waiting on the same attempt, should not have called the task again
    assertThat(reconnectionTask.callCount()).isEqualTo(1);
    // because we were still in progress all the time, to the outside it's as if the stop/restart
    // had never happened
    verifyNoMoreInteractions(onStartCallback);
    verifyNoMoreInteractions(onStopCallback);

    // When
    reconnectionTask.complete(true);
    runPendingTasks();

    // Then
    assertThat(reconnection.isRunning()).isFalse();
    verify(onStopCallback).run();
  }

  private void runPendingTasks() {
    channel.runPendingTasks();
  }

  private static class MockReconnectionTask implements Callable<CompletionStage<Boolean>> {
    private volatile CompletableFuture<Boolean> nextResult;
    private final AtomicInteger callCount = new AtomicInteger();

    @Override
    public CompletionStage<Boolean> call() throws Exception {
      assertThat(nextResult == null || nextResult.isDone()).isTrue();
      callCount.incrementAndGet();
      nextResult = new CompletableFuture<>();
      return nextResult;
    }

    private void complete(boolean outcome) {
      assertThat(nextResult != null || !nextResult.isDone()).isTrue();
      nextResult.complete(outcome);
      nextResult = null;
    }

    private int callCount() {
      return callCount.get();
    }
  }
}
