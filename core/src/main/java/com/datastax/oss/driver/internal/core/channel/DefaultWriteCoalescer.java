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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import net.jcip.annotations.ThreadSafe;
import org.jctools.queues.atomic.MpscLinkedAtomicQueue;

/**
 * Default write coalescing strategy.
 *
 * <p>It maintains a queue per event loop, with the writes targeting the channels that run on this
 * loop. As soon as a write gets enqueued, it triggers a task that will flush the queue (other
 * writes can get enqueued before the task runs). Once that task is complete, it re-triggers itself
 * as long as new writes have been enqueued, or {@code maxRunsWithNoWork} times if there are no more
 * tasks.
 *
 * <p>Note that Netty provides a similar mechanism out of the box ({@link
 * io.netty.handler.flush.FlushConsolidationHandler}), but in our experience our approach allows
 * more performance gains, because it allows consolidating not only the flushes, but also the write
 * tasks themselves (a single consolidated write task is scheduled on the event loop, instead of
 * multiple individual tasks, so there is less context switching).
 */
@ThreadSafe
public class DefaultWriteCoalescer implements WriteCoalescer {
  private final int maxRunsWithNoWork;
  private final long rescheduleIntervalNanos;
  private final ConcurrentMap<EventLoop, Flusher> flushers = new ConcurrentHashMap<>();

  public DefaultWriteCoalescer(DriverContext context) {
    DriverConfigProfile config = context.config().getDefaultProfile();
    this.maxRunsWithNoWork = config.getInt(DefaultDriverOption.COALESCER_MAX_RUNS);
    this.rescheduleIntervalNanos =
        config.getDuration(DefaultDriverOption.COALESCER_INTERVAL).toNanos();
  }

  @Override
  public ChannelFuture writeAndFlush(Channel channel, Object message) {
    ChannelPromise writePromise = channel.newPromise();
    Write write = new Write(channel, message, writePromise);
    enqueue(write, channel.eventLoop());
    return writePromise;
  }

  private void enqueue(Write write, EventLoop eventLoop) {
    Flusher flusher = flushers.computeIfAbsent(eventLoop, Flusher::new);
    flusher.enqueue(write);
  }

  private class Flusher {
    private final EventLoop eventLoop;

    // These variables are accessed both from client threads and the event loop
    private final Queue<Write> writes = new MpscLinkedAtomicQueue<>();
    private final AtomicBoolean running = new AtomicBoolean();

    // These variables are accessed only from runOnEventLoop, they don't need to be thread-safe
    private final Set<Channel> channels = new HashSet<>();
    private int runsWithNoWork = 0;

    private Flusher(EventLoop eventLoop) {
      this.eventLoop = eventLoop;
    }

    private void enqueue(Write write) {
      boolean added = writes.offer(write);
      assert added; // always true (see MpscLinkedAtomicQueue implementation)
      if (running.compareAndSet(false, true)) {
        eventLoop.execute(this::runOnEventLoop);
      }
    }

    private void runOnEventLoop() {
      assert eventLoop.inEventLoop();

      boolean didSomeWork = false;
      Write write;
      while ((write = writes.poll()) != null) {
        Channel channel = write.channel;
        channels.add(channel);
        channel.write(write.message, write.writePromise);
        didSomeWork = true;
      }

      for (Channel channel : channels) {
        channel.flush();
      }
      channels.clear();

      if (didSomeWork) {
        runsWithNoWork = 0;
      } else if (++runsWithNoWork > maxRunsWithNoWork) {
        // Prepare to stop
        running.set(false);
        // If no new writes have been enqueued since the previous line, we can return safely
        if (writes.isEmpty()) {
          return;
        }
        // Otherwise check if those writes have triggered a new run. If not, we need to do that
        // ourselves (i.e. not return yet)
        if (!running.compareAndSet(false, true)) {
          return;
        }
      }
      if (!eventLoop.isShuttingDown()) {
        eventLoop.schedule(this::runOnEventLoop, rescheduleIntervalNanos, TimeUnit.NANOSECONDS);
      }
    }
  }

  private static class Write {
    private final Channel channel;
    private final Object message;
    private final ChannelPromise writePromise;

    private Write(Channel channel, Object message, ChannelPromise writePromise) {
      this.channel = channel;
      this.message = message;
      this.writePromise = writePromise;
    }
  }
}
