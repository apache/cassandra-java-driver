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

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import net.jcip.annotations.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Debounces a sequence of events to smoothen temporary oscillations.
 *
 * <p>When a first event is received, the debouncer starts a time window. If no other event is
 * received within that window, the initial event is flushed. However, if another event arrives, the
 * window is reset, and the next flush will now contain both events. If the window keeps getting
 * reset, the debouncer will flush after a given number of accumulated events.
 *
 * @param <IncomingT> the type of the incoming events.
 * @param <CoalescedT> the resulting type after the events of a batch have been coalesced.
 */
@NotThreadSafe // must be confined to adminExecutor
public class Debouncer<IncomingT, CoalescedT> {
  private static final Logger LOG = LoggerFactory.getLogger(Debouncer.class);

  private final String logPrefix;
  private final EventExecutor adminExecutor;
  private final Consumer<CoalescedT> onFlush;
  private final Duration window;
  private final long maxEvents;
  private final Function<List<IncomingT>, CoalescedT> coalescer;

  private List<IncomingT> currentBatch = new ArrayList<>();
  private ScheduledFuture<?> nextFlush;
  private boolean stopped;

  /**
   * Creates a new instance.
   *
   * @param adminExecutor the executor that will be used to schedule all tasks.
   * @param coalescer how to transform a batch of events into a result.
   * @param onFlush what to do with a result.
   * @param window the time window.
   * @param maxEvents the maximum number of accumulated events before a flush is forced.
   */
  public Debouncer(
      EventExecutor adminExecutor,
      Function<List<IncomingT>, CoalescedT> coalescer,
      Consumer<CoalescedT> onFlush,
      Duration window,
      long maxEvents) {
    this("debouncer", adminExecutor, coalescer, onFlush, window, maxEvents);
  }

  /**
   * Creates a new instance.
   *
   * @param logPrefix the log prefix to use in log messages.
   * @param adminExecutor the executor that will be used to schedule all tasks.
   * @param coalescer how to transform a batch of events into a result.
   * @param onFlush what to do with a result.
   * @param window the time window.
   * @param maxEvents the maximum number of accumulated events before a flush is forced.
   */
  public Debouncer(
      String logPrefix,
      EventExecutor adminExecutor,
      Function<List<IncomingT>, CoalescedT> coalescer,
      Consumer<CoalescedT> onFlush,
      Duration window,
      long maxEvents) {
    this.logPrefix = logPrefix;
    this.coalescer = coalescer;
    Preconditions.checkArgument(maxEvents >= 1, "maxEvents should be at least 1");
    this.adminExecutor = adminExecutor;
    this.onFlush = onFlush;
    this.window = window;
    this.maxEvents = maxEvents;
  }

  /** This must be called on eventExecutor too. */
  public void receive(IncomingT element) {
    assert adminExecutor.inEventLoop();
    if (stopped) {
      return;
    }
    if (window.isZero() || maxEvents == 1) {
      LOG.debug(
          "[{}] Received {}, flushing immediately (window = {}, maxEvents = {})",
          logPrefix,
          element,
          window,
          maxEvents);
      onFlush.accept(coalescer.apply(ImmutableList.of(element)));
    } else {
      currentBatch.add(element);
      if (currentBatch.size() == maxEvents) {
        LOG.debug(
            "[{}] Received {}, flushing immediately (because {} accumulated events)",
            logPrefix,
            element,
            maxEvents);
        flushNow();
      } else {
        LOG.debug("[{}] Received {}, scheduling next flush in {}", logPrefix, element, window);
        scheduleFlush();
      }
    }
  }

  public void flushNow() {
    assert adminExecutor.inEventLoop();
    LOG.debug("[{}] Flushing now", logPrefix);
    cancelNextFlush();
    if (!currentBatch.isEmpty()) {
      onFlush.accept(coalescer.apply(currentBatch));
      currentBatch = new ArrayList<>();
    }
  }

  private void scheduleFlush() {
    assert adminExecutor.inEventLoop();
    cancelNextFlush();
    nextFlush = adminExecutor.schedule(this::flushNow, window.toNanos(), TimeUnit.NANOSECONDS);
    nextFlush.addListener(UncaughtExceptions::log);
  }

  private void cancelNextFlush() {
    assert adminExecutor.inEventLoop();
    if (nextFlush != null && !nextFlush.isDone()) {
      boolean cancelled = nextFlush.cancel(true);
      if (cancelled) {
        LOG.debug("[{}] Cancelled existing scheduled flush", logPrefix);
      }
    }
  }

  /**
   * Stop debouncing: the next flush is cancelled, and all pending and future events will be
   * ignored.
   */
  public void stop() {
    assert adminExecutor.inEventLoop();
    if (!stopped) {
      stopped = true;
      cancelNextFlush();
    }
  }
}
