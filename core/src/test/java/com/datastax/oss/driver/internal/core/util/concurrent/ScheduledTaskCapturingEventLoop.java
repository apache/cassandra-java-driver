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
package com.datastax.oss.driver.internal.core.util.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Extend Netty's default event loop to capture scheduled tasks instead of running them. The tasks
 * can be checked later, and run manually.
 *
 * <p>Tasks submitted with {@link #execute(Runnable)} or {@link #submit(Callable)} are still
 * executed normally.
 *
 * <p>This is used to make unit tests independent of time.
 */
@SuppressWarnings("FunctionalInterfaceClash") // does not matter for test code
public class ScheduledTaskCapturingEventLoop extends DefaultEventLoop {

  private final BlockingQueue<CapturedTask<?>> capturedTasks = new ArrayBlockingQueue<>(100);

  public ScheduledTaskCapturingEventLoop(EventLoopGroup parent) {
    super(parent);
  }

  @NonNull
  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    CapturedTask<V> task = new CapturedTask<>(callable, delay, unit);
    boolean added = capturedTasks.offer(task);
    assertThat(added).isTrue();
    return task.scheduledFuture;
  }

  @NonNull
  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    return schedule(
        () -> {
          command.run();
          return null;
        },
        delay,
        unit);
  }

  @NonNull
  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      Runnable command, long initialDelay, long period, TimeUnit unit) {
    CapturedTask<?> task =
        new CapturedTask<>(
            () -> {
              command.run();
              return null;
            },
            initialDelay,
            period,
            unit);
    boolean added = capturedTasks.offer(task);
    assertThat(added).isTrue();
    return task.scheduledFuture;
  }

  @NonNull
  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
      Runnable command, long initialDelay, long delay, TimeUnit unit) {
    throw new UnsupportedOperationException("Not supported yet");
  }

  public CapturedTask<?> nextTask() {
    try {
      return capturedTasks.poll(100, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      fail("Unexpected interruption", e);
      throw new AssertionError();
    }
  }

  /**
   * Wait for any pending non-scheduled task (submitted with {@code submit}, {@code execute}, etc.)
   * to complete.
   */
  public void waitForNonScheduledTasks() {
    ScheduledFuture<Object> f = super.schedule(() -> null, 5, TimeUnit.NANOSECONDS);
    try {
      Uninterruptibles.getUninterruptibly(f, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      fail("unexpected error", e.getCause());
    } catch (TimeoutException e) {
      fail("timed out while waiting for admin tasks to complete", e);
    }
  }

  public class CapturedTask<V> {
    private final FutureTask<V> futureTask;
    private final long initialDelay;
    private final long period;
    private final TimeUnit unit;

    @SuppressWarnings("unchecked")
    private final ScheduledFuture<V> scheduledFuture = mock(ScheduledFuture.class);

    CapturedTask(Callable<V> task, long initialDelay, TimeUnit unit) {
      this(task, initialDelay, -1, unit);
    }

    CapturedTask(Callable<V> task, long initialDelay, long period, TimeUnit unit) {
      this.futureTask = new FutureTask<>(task);
      this.initialDelay = initialDelay;
      this.period = period;
      this.unit = unit;

      // If the code under test cancels the scheduled future, cancel our task
      when(scheduledFuture.cancel(anyBoolean()))
          .thenAnswer(invocation -> futureTask.cancel(invocation.getArgument(0)));

      // Delegate methods of the scheduled future to our task (to be extended to more methods if
      // needed)
      when(scheduledFuture.isDone()).thenAnswer(invocation -> futureTask.isDone());
      when(scheduledFuture.isCancelled()).thenAnswer(invocation -> futureTask.isCancelled());
    }

    public void run() {
      submit(futureTask);
      waitForNonScheduledTasks();
    }

    public boolean isCancelled() {
      // futureTask.isCancelled() can create timing issues in CI environments, so give the
      // cancellation a short time to complete instead:
      try {
        futureTask.get(3, TimeUnit.SECONDS);
      } catch (CancellationException e) {
        return true;
      } catch (Exception e) {
        // ignore
      }
      return false;
    }

    public long getInitialDelay(TimeUnit targetUnit) {
      return targetUnit.convert(initialDelay, unit);
    }

    /** By convention, non-recurring tasks have a negative period */
    public long getPeriod(TimeUnit targetUnit) {
      return targetUnit.convert(period, unit);
    }
  }
}
