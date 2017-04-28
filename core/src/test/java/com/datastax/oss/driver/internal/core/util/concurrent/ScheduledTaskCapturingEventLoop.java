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

import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;

/**
 * Extend Netty's default event loop to capture scheduled tasks instead of running them. The tasks
 * can be checked later, and run manually.
 *
 * <p>This is used to make unit tests independent of time.
 */
public class ScheduledTaskCapturingEventLoop extends DefaultEventLoop {

  private final Queue<CapturedTask> capturedTasks = new ConcurrentLinkedQueue<>();

  public ScheduledTaskCapturingEventLoop(EventLoopGroup parent) {
    super(parent);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    CapturedTask<V> task = new CapturedTask<>(callable, delay, unit);
    boolean added = capturedTasks.offer(task);
    assertThat(added).isTrue();
    return task.scheduledFuture;
  }

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

  public CapturedTask<?> nextTask() {
    return capturedTasks.poll();
  }

  public static class CapturedTask<V> {
    private final FutureTask<V> futureTask;
    private final long delay;
    private final TimeUnit unit;

    @SuppressWarnings("unchecked")
    private final ScheduledFuture<V> scheduledFuture = Mockito.mock(ScheduledFuture.class);

    CapturedTask(Callable<V> task, long delay, TimeUnit unit) {
      this.futureTask = new FutureTask<>(task);
      this.delay = delay;
      this.unit = unit;

      // If the code under test cancels the scheduled future, cancel our task
      Mockito.when(scheduledFuture.cancel(anyBoolean()))
          .thenAnswer(invocation -> futureTask.cancel(invocation.getArgument(0)));

      // Delegate methods of the scheduled future to our task (to be extended to more methods if
      // needed)
      Mockito.when(scheduledFuture.isDone()).thenAnswer(invocation -> futureTask.isDone());
      Mockito.when(scheduledFuture.isCancelled())
          .thenAnswer(invocation -> futureTask.isCancelled());
    }

    public void run() {
      futureTask.run();
    }

    public boolean isCancelled() {
      return futureTask.isCancelled();
    }

    public long getDelay(TimeUnit targetUnit) {
      return targetUnit.convert(delay, unit);
    }
  }
}
