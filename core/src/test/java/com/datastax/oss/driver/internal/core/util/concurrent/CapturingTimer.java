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

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of Netty's {@link io.netty.util.Timer Timer} interface to capture scheduled {@link
 * io.netty.util.Timeout Timeouts} instead of running them, so they can be run manually in tests.
 */
public class CapturingTimer implements Timer {

  private final ArrayBlockingQueue<CapturedTimeout> timeoutQueue = new ArrayBlockingQueue<>(16);

  @Override
  public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
    // delay and unit are not needed as the Timeout's TimerTask will be run manually
    CapturedTimeout timeout = new CapturedTimeout(task, this, delay, unit);
    // add the timeout to the queue
    timeoutQueue.add(timeout);
    return timeout;
  }

  /**
   * Retrieves the next scheduled Timeout. In tests, this will usually be a request timeout or a
   * speculative execution. Tests will need be able to predict the ordering as it is not easy to
   * tell from the returned Timeout itself.
   */
  public CapturedTimeout getNextTimeout() {
    return timeoutQueue.poll();
  }

  @Override
  public Set<Timeout> stop() {
    if (timeoutQueue.isEmpty()) {
      return Collections.emptySet();
    }
    Set<Timeout> timeoutsRemaining = new HashSet<>(timeoutQueue.size());
    for (Timeout t : timeoutQueue) {
      if (t != null) {
        t.cancel();
        timeoutsRemaining.add(t);
      }
    }
    return timeoutsRemaining;
  }

  /**
   * Implementation of Netty's {@link io.netty.util.Timeout Timeout} interface. It is just a simple
   * class that keeps track of the {@link io.netty.util.TimerTask TimerTask} and the {@link
   * io.netty.util.Timer Timer} implementation that should only be used in tests. The intended use
   * is to call the {@link io.netty.util.TimerTask#run(io.netty.util.Timeout) run()} method on the
   * TimerTask when you want to execute the task (so you don't have to depend on a real timer).
   *
   * <p>Example:
   *
   * <pre>{@code
   * // get the next timeout from the timer
   * Timeout t = timer.getNextTimeout();
   * // run the TimerTask associated with the timeout
   * t.task.run(t);
   * }</pre>
   */
  public static class CapturedTimeout implements Timeout {

    private final TimerTask task;
    private final CapturingTimer timer;
    private final long delay;
    private final TimeUnit unit;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    private CapturedTimeout(TimerTask task, CapturingTimer timer, long delay, TimeUnit unit) {
      this.task = task;
      this.timer = timer;
      this.delay = delay;
      this.unit = unit;
    }

    @Override
    public Timer timer() {
      return timer;
    }

    @Override
    public TimerTask task() {
      return task;
    }

    public long getDelay(TimeUnit targetUnit) {
      return targetUnit.convert(delay, unit);
    }

    @Override
    public boolean isExpired() {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return cancelled.get();
    }

    @Override
    public boolean cancel() {
      return cancelled.compareAndSet(false, true);
    }
  }
}
