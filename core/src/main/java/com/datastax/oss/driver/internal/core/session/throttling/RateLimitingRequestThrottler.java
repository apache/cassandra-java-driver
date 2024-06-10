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
package com.datastax.oss.driver.internal.core.session.throttling;

import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.EventExecutor;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A request throttler that limits the rate of requests per second.
 *
 * <p>To activate this throttler, modify the {@code advanced.throttler} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.throttler {
 *     class = RateLimitingRequestThrottler
 *     max-requests-per-second = 10000
 *     max-queue-size = 10000
 *     drain-interval = 10 milliseconds
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
@ThreadSafe
public class RateLimitingRequestThrottler implements RequestThrottler {

  private static final Logger LOG = LoggerFactory.getLogger(RateLimitingRequestThrottler.class);

  private final String logPrefix;
  private final NanoClock clock;
  private final int maxRequestsPerSecond;
  private final int maxQueueSize;
  private final long drainIntervalNanos;
  private final EventExecutor scheduler;

  private final ReentrantLock lock = new ReentrantLock();

  @GuardedBy("lock")
  private long lastUpdateNanos;

  @GuardedBy("lock")
  private int storedPermits;

  @GuardedBy("lock")
  private final Deque<Throttled> queue = new ArrayDeque<>();

  @GuardedBy("lock")
  private boolean closed;

  @SuppressWarnings("unused")
  public RateLimitingRequestThrottler(DriverContext context) {
    this(context, System::nanoTime);
  }

  @VisibleForTesting
  RateLimitingRequestThrottler(DriverContext context, NanoClock clock) {
    this.logPrefix = context.getSessionName();
    this.clock = clock;

    DriverExecutionProfile config = context.getConfig().getDefaultProfile();

    this.maxRequestsPerSecond =
        config.getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND);
    this.maxQueueSize = config.getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE);
    Duration drainInterval =
        config.getDuration(DefaultDriverOption.REQUEST_THROTTLER_DRAIN_INTERVAL);
    this.drainIntervalNanos = drainInterval.toNanos();

    this.lastUpdateNanos = clock.nanoTime();
    // Start with one second worth of permits to avoid delaying initial requests
    this.storedPermits = maxRequestsPerSecond;

    this.scheduler =
        ((InternalDriverContext) context).getNettyOptions().adminEventExecutorGroup().next();

    LOG.debug(
        "[{}] Initializing with maxRequestsPerSecond = {}, maxQueueSize = {}, drainInterval = {}",
        logPrefix,
        maxRequestsPerSecond,
        maxQueueSize,
        drainInterval);
  }

  @Override
  public void register(@NonNull Throttled request) {
    long now = clock.nanoTime();
    lock.lock();
    try {
      if (closed) {
        LOG.trace("[{}] Rejecting request after shutdown", logPrefix);
        fail(request, "The session is shutting down");
      } else if (queue.isEmpty() && acquire(now, 1) == 1) {
        LOG.trace("[{}] Starting newly registered request", logPrefix);
        request.onThrottleReady(false);
      } else if (queue.size() < maxQueueSize) {
        LOG.trace("[{}] Enqueuing request", logPrefix);
        if (queue.isEmpty()) {
          scheduler.schedule(this::drain, drainIntervalNanos, TimeUnit.NANOSECONDS);
        }
        queue.add(request);
      } else {
        LOG.trace("[{}] Rejecting request because of full queue", logPrefix);
        fail(
            request,
            String.format(
                "The session has reached its maximum capacity "
                    + "(requests/s: %d, queue size: %d)",
                maxRequestsPerSecond, maxQueueSize));
      }
    } finally {
      lock.unlock();
    }
  }

  // Runs periodically when the queue is not empty. It tries to dequeue as much as possible while
  // staying under the target rate. If it does not completely drain the queue, it reschedules
  // itself.
  private void drain() {
    assert scheduler.inEventLoop();
    long now = clock.nanoTime();
    lock.lock();
    try {
      if (closed || queue.isEmpty()) {
        return;
      }
      int toDequeue = acquire(now, queue.size());
      LOG.trace("[{}] Dequeuing {}/{} elements", logPrefix, toDequeue, queue.size());
      for (int i = 0; i < toDequeue; i++) {
        LOG.trace("[{}] Starting dequeued request", logPrefix);
        queue.poll().onThrottleReady(true);
      }
      if (!queue.isEmpty()) {
        LOG.trace(
            "[{}] {} elements remaining in queue, rescheduling drain task",
            logPrefix,
            queue.size());
        scheduler.schedule(this::drain, drainIntervalNanos, TimeUnit.NANOSECONDS);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void signalSuccess(@NonNull Throttled request) {
    // nothing to do
  }

  @Override
  public void signalError(@NonNull Throttled request, @NonNull Throwable error) {
    // nothing to do
  }

  @Override
  public void signalTimeout(@NonNull Throttled request) {
    lock.lock();
    try {
      if (!closed && queue.remove(request)) { // The request timed out before it was active
        LOG.trace("[{}] Removing timed out request from the queue", logPrefix);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() {
    lock.lock();
    try {
      closed = true;
      LOG.debug("[{}] Rejecting {} queued requests after shutdown", logPrefix, queue.size());
      for (Throttled request : queue) {
        fail(request, "The session is shutting down");
      }
    } finally {
      lock.unlock();
    }
  }

  @SuppressWarnings("GuardedBy") // this method is only called with the lock held
  private int acquire(long currentTimeNanos, int wantedPermits) {
    assert lock.isHeldByCurrentThread() && !closed;

    long elapsedNanos = currentTimeNanos - lastUpdateNanos;

    if (elapsedNanos >= 1_000_000_000) {
      // created more than the max, so whatever was stored, the sum will be capped to the max
      storedPermits = maxRequestsPerSecond;
      lastUpdateNanos = currentTimeNanos;
    } else if (elapsedNanos > 0) {
      int createdPermits = (int) (elapsedNanos * maxRequestsPerSecond / 1_000_000_000);
      if (createdPermits > 0) {
        // Only reset interval if we've generated permits, otherwise we might continually reset
        // before we get the chance to generate anything.
        lastUpdateNanos = currentTimeNanos;
      }
      storedPermits = Math.min(storedPermits + createdPermits, maxRequestsPerSecond);
    }

    int returned = (storedPermits >= wantedPermits) ? wantedPermits : storedPermits;
    storedPermits = Math.max(storedPermits - wantedPermits, 0);
    return returned;
  }

  public int getQueueSize() {
    lock.lock();
    try {
      return queue.size();
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  int getStoredPermits() {
    lock.lock();
    try {
      return storedPermits;
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  Deque<Throttled> getQueue() {
    lock.lock();
    try {
      return queue;
    } finally {
      lock.unlock();
    }
  }

  private static void fail(Throttled request, String message) {
    request.onThrottleFailure(new RequestThrottlingException(message));
  }
}
