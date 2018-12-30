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

import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy.ReconnectionSchedule;
import com.datastax.oss.driver.internal.core.util.Loggers;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import net.jcip.annotations.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reconnection process that, if failed, is retried periodically according to the intervals
 * defined by a policy.
 *
 * <p>All the tasks run on a Netty event executor that is provided at construction time. Clients are
 * also expected to call the public methods on that thread.
 */
@NotThreadSafe // must be confined to executor
public class Reconnection {
  private static final Logger LOG = LoggerFactory.getLogger(Reconnection.class);

  private enum State {
    STOPPED,
    SCHEDULED, // next attempt scheduled but not started yet
    ATTEMPT_IN_PROGRESS, // current attempt started and not completed yet
    STOP_AFTER_CURRENT, // stopped, but we're letting an in-progress attempt finish
    ;
  }

  private final String logPrefix;
  private final EventExecutor executor;
  private final Supplier<ReconnectionSchedule> scheduleSupplier;
  private final Callable<CompletionStage<Boolean>> reconnectionTask;
  private final Runnable onStart;
  private final Runnable onStop;

  private State state = State.STOPPED;
  private ReconnectionSchedule reconnectionSchedule;
  private ScheduledFuture<CompletionStage<Boolean>> nextAttempt;

  /**
   * @param reconnectionTask the actual thing to try on a reconnection, returns if it succeeded or
   *     not.
   */
  public Reconnection(
      String logPrefix,
      EventExecutor executor,
      Supplier<ReconnectionSchedule> scheduleSupplier,
      Callable<CompletionStage<Boolean>> reconnectionTask,
      Runnable onStart,
      Runnable onStop) {
    this.logPrefix = logPrefix;
    this.executor = executor;
    this.scheduleSupplier = scheduleSupplier;
    this.reconnectionTask = reconnectionTask;
    this.onStart = onStart;
    this.onStop = onStop;
  }

  public Reconnection(
      String logPrefix,
      EventExecutor executor,
      Supplier<ReconnectionSchedule> scheduleSupplier,
      Callable<CompletionStage<Boolean>> reconnectionTask) {
    this(logPrefix, executor, scheduleSupplier, reconnectionTask, () -> {}, () -> {});
  }

  /**
   * Note that if {@link #stop()} was called but we're still waiting for the last pending attempt to
   * complete, this still returns {@code true}.
   */
  public boolean isRunning() {
    assert executor.inEventLoop();
    return state != State.STOPPED;
  }

  /** This is a no-op if the reconnection is already running. */
  public void start() {
    start(null);
  }

  public void start(ReconnectionSchedule customSchedule) {
    assert executor.inEventLoop();
    switch (state) {
      case SCHEDULED:
      case ATTEMPT_IN_PROGRESS:
        // nothing to do
        break;
      case STOP_AFTER_CURRENT:
        // cancel the scheduled stop
        state = State.ATTEMPT_IN_PROGRESS;
        break;
      case STOPPED:
        reconnectionSchedule = (customSchedule == null) ? scheduleSupplier.get() : customSchedule;
        onStart.run();
        scheduleNextAttempt();
        break;
    }
  }

  /**
   * Forces a reconnection now, without waiting for the next scheduled attempt.
   *
   * @param forceIfStopped if true and the reconnection is not running, it will get started (meaning
   *     subsequent reconnections will be scheduled if this attempt fails). If false and the
   *     reconnection is not running, no attempt is scheduled.
   */
  public void reconnectNow(boolean forceIfStopped) {
    assert executor.inEventLoop();
    if (state == State.ATTEMPT_IN_PROGRESS || state == State.STOP_AFTER_CURRENT) {
      LOG.debug(
          "[{}] reconnectNow and current attempt was still running, letting it complete",
          logPrefix);
      if (state == State.STOP_AFTER_CURRENT) {
        // Make sure that we will schedule other attempts if this one fails.
        state = State.ATTEMPT_IN_PROGRESS;
      }
    } else if (state == State.STOPPED && !forceIfStopped) {
      LOG.debug("[{}] reconnectNow(false) while stopped, nothing to do", logPrefix);
    } else {
      assert state == State.SCHEDULED || (state == State.STOPPED && forceIfStopped);
      LOG.debug("[{}] Forcing next attempt now", logPrefix);
      if (nextAttempt != null) {
        nextAttempt.cancel(true);
      }
      try {
        onNextAttemptStarted(reconnectionTask.call());
      } catch (Exception e) {
        Loggers.warnWithException(
            LOG, "[{}] Uncaught error while starting reconnection attempt", logPrefix, e);
        scheduleNextAttempt();
      }
    }
  }

  public void stop() {
    assert executor.inEventLoop();
    switch (state) {
      case STOPPED:
      case STOP_AFTER_CURRENT:
        break;
      case ATTEMPT_IN_PROGRESS:
        state = State.STOP_AFTER_CURRENT;
        break;
      case SCHEDULED:
        reallyStop();
        break;
    }
  }

  private void reallyStop() {
    LOG.debug("[{}] Stopping reconnection", logPrefix);
    state = State.STOPPED;
    if (nextAttempt != null) {
      nextAttempt.cancel(true);
      nextAttempt = null;
    }
    onStop.run();
    reconnectionSchedule = null;
  }

  private void scheduleNextAttempt() {
    assert executor.inEventLoop();
    state = State.SCHEDULED;
    if (reconnectionSchedule == null) { // happens if reconnectNow() while we were stopped
      reconnectionSchedule = scheduleSupplier.get();
    }
    Duration nextInterval = reconnectionSchedule.nextDelay();
    LOG.debug("[{}] Scheduling next reconnection in {}", logPrefix, nextInterval);
    nextAttempt = executor.schedule(reconnectionTask, nextInterval.toNanos(), TimeUnit.NANOSECONDS);
    nextAttempt.addListener(
        (Future<CompletionStage<Boolean>> f) -> {
          if (f.isSuccess()) {
            onNextAttemptStarted(f.getNow());
          } else if (!f.isCancelled()) {
            Loggers.warnWithException(
                LOG,
                "[{}] Uncaught error while starting reconnection attempt",
                logPrefix,
                f.cause());
            scheduleNextAttempt();
          }
        });
  }

  // When the Callable runs this means the caller has started the attempt, we have yet to wait on
  // the CompletableFuture to find out if that succeeded or not.
  private void onNextAttemptStarted(CompletionStage<Boolean> futureOutcome) {
    assert executor.inEventLoop();
    state = State.ATTEMPT_IN_PROGRESS;
    futureOutcome
        .whenCompleteAsync(this::onNextAttemptCompleted, executor)
        .exceptionally(UncaughtExceptions::log);
  }

  private void onNextAttemptCompleted(Boolean success, Throwable error) {
    assert executor.inEventLoop();
    if (success) {
      LOG.debug("[{}] Reconnection successful", logPrefix);
      reallyStop();
    } else {
      if (error != null && !(error instanceof CancellationException)) {
        Loggers.warnWithException(
            LOG, "[{}] Uncaught error while starting reconnection attempt", logPrefix, error);
      }
      if (state == State.STOP_AFTER_CURRENT) {
        reallyStop();
      } else {
        assert state == State.ATTEMPT_IN_PROGRESS;
        scheduleNextAttempt();
      }
    }
  }
}
