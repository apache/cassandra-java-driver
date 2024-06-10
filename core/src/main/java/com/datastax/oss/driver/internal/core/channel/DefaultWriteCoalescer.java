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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import net.jcip.annotations.ThreadSafe;

/**
 * Default write coalescing strategy.
 *
 * <p>It maintains a queue per event loop, with the writes targeting the channels that run on this
 * loop. As soon as a write gets enqueued, it triggers a task that will flush the queue (other
 * writes may get enqueued before or while the task runs).
 *
 * <p>Note that Netty provides a similar mechanism out of the box ({@link
 * io.netty.handler.flush.FlushConsolidationHandler}), but in our experience our approach allows
 * more performance gains, because it allows consolidating not only the flushes, but also the write
 * tasks themselves (a single consolidated write task is scheduled on the event loop, instead of
 * multiple individual tasks, so there is less context switching).
 */
@ThreadSafe
public class DefaultWriteCoalescer implements WriteCoalescer {
  private final long rescheduleIntervalNanos;
  private final ConcurrentMap<EventLoop, Flusher> flushers = new ConcurrentHashMap<>();

  public DefaultWriteCoalescer(DriverContext context) {
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    rescheduleIntervalNanos = config.getDuration(DefaultDriverOption.COALESCER_INTERVAL).toNanos();
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
    private final Queue<Write> writes = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean running = new AtomicBoolean();

    // This variable is accessed only from runOnEventLoop, it doesn't need to be thread-safe
    private final Set<Channel> channels = new HashSet<>();

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

      Write write;
      while ((write = writes.poll()) != null) {
        Channel channel = write.channel;
        channels.add(channel);
        channel.write(write.message, write.writePromise);
      }

      for (Channel channel : channels) {
        channel.flush();
      }
      channels.clear();

      // Prepare to stop
      running.set(false);

      // enqueue() can be called concurrently with this method. There is a race condition if it:
      // - added an element in the queue after we were done draining it
      // - but observed running==true before we flipped it, and therefore didn't schedule another
      //   run

      // If nothing was added in the queue, there were no concurrent calls, we can stop safely now
      if (writes.isEmpty()) {
        return;
      }

      // Otherwise, check if one of those calls scheduled a run. If so, they flipped the bit back
      // on. If not, we need to do it ourselves.
      boolean shouldRestartMyself = running.compareAndSet(false, true);

      if (shouldRestartMyself && !eventLoop.isShuttingDown()) {
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
