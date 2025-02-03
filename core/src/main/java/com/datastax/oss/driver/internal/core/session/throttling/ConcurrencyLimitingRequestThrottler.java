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
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A request throttler that limits the number of concurrent requests.
 *
 * <p>To activate this throttler, modify the {@code advanced.throttler} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.throttler {
 *     class = ConcurrencyLimitingRequestThrottler
 *     max-concurrent-requests = 10000
 *     max-queue-size = 10000
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
@ThreadSafe
public class ConcurrencyLimitingRequestThrottler implements RequestThrottler {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConcurrencyLimitingRequestThrottler.class);

  private final String logPrefix;
  private final int maxConcurrentRequests;
  private final int maxQueueSize;
  private final AtomicInteger concurrentRequests = new AtomicInteger(0);
  // CLQ is not O(1) for size(), as it forces a full iteration of the queue. So, we track
  // the size of the queue explicitly.
  private final Deque<Throttled> queue = new ConcurrentLinkedDeque<>();
  private final AtomicInteger queueSize = new AtomicInteger(0);
  private volatile boolean closed = false;

  public ConcurrencyLimitingRequestThrottler(DriverContext context) {
    this.logPrefix = context.getSessionName();
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    this.maxConcurrentRequests =
        config.getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS);
    this.maxQueueSize = config.getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE);
    LOG.debug(
        "[{}] Initializing with maxConcurrentRequests = {}, maxQueueSize = {}",
        logPrefix,
        maxConcurrentRequests,
        maxQueueSize);
  }

  @Override
  public void register(@NonNull Throttled request) {
    if (closed) {
      LOG.trace("[{}] Rejecting request after shutdown", logPrefix);
      fail(request, "The session is shutting down");
      return;
    }

    // Implementation note: Technically the "concurrent requests" or "queue size"
    // could read transiently over the limit, but the queue itself will never grow
    // beyond the limit since we always check for that condition and revert if
    // over-limit. We do this instead of a CAS-loop to avoid the potential loop.

    // If no backlog exists AND we get capacity, we can execute immediately
    if (queueSize.get() == 0) {
      // Take a claim first, and then check if we are OK to proceed
      int newConcurrent = concurrentRequests.incrementAndGet();
      if (newConcurrent <= maxConcurrentRequests) {
        LOG.trace("[{}] Starting newly registered request", logPrefix);
        request.onThrottleReady(false);
        return;
      } else {
        // We exceeded the limit, decrement the count and fall through to the queuing logic
        concurrentRequests.decrementAndGet();
      }
    }

    // If we have a backlog, or we failed to claim capacity, try to enqueue
    int newQueueSize = queueSize.incrementAndGet();
    if (newQueueSize <= maxQueueSize) {
      LOG.trace("[{}] Enqueuing request", logPrefix);
      queue.offer(request);

      // Double-check that we were still supposed to be enqueued; it is possible
      // that the session was closed while we were enqueuing, it's also possible
      // that it is right now removing the request, so we need to check both
      if (closed) {
        if (queue.remove(request)) {
          queueSize.decrementAndGet();
          LOG.trace("[{}] Rejecting late request after shutdown", logPrefix);
          fail(request, "The session is shutting down");
        }
      }
    } else {
      LOG.trace("[{}] Rejecting request because of full queue", logPrefix);
      queueSize.decrementAndGet();
      fail(
          request,
          String.format(
              "The session has reached its maximum capacity "
                  + "(concurrent requests: %d, queue size: %d)",
              maxConcurrentRequests, maxQueueSize));
    }
  }

  @Override
  public void signalSuccess(@NonNull Throttled request) {
    Throttled nextRequest = onRequestDoneAndDequeNext();
    if (nextRequest != null) {
      nextRequest.onThrottleReady(true);
    }
  }

  @Override
  public void signalError(@NonNull Throttled request, @NonNull Throwable error) {
    signalSuccess(request); // not treated differently
  }

  @Override
  public void signalTimeout(@NonNull Throttled request) {
    Throttled nextRequest = null;
    if (!closed) {
      if (queue.remove(request)) { // The request timed out before it was active
        queueSize.decrementAndGet();
        LOG.trace("[{}] Removing timed out request from the queue", logPrefix);
      } else {
        nextRequest = onRequestDoneAndDequeNext();
      }
    }

    if (nextRequest != null) {
      nextRequest.onThrottleReady(true);
    }
  }

  @Override
  public void signalCancel(@NonNull Throttled request) {
    Throttled nextRequest = null;
    if (!closed) {
      if (queue.remove(request)) { // The request has been cancelled before it was active
        queueSize.decrementAndGet();
        LOG.trace("[{}] Removing cancelled request from the queue", logPrefix);
      } else {
        nextRequest = onRequestDoneAndDequeNext();
      }
    }

    if (nextRequest != null) {
      nextRequest.onThrottleReady(true);
    }
  }

  @Nullable
  private Throttled onRequestDoneAndDequeNext() {
    if (!closed) {
      Throttled nextRequest = queue.poll();
      if (nextRequest == null) {
        concurrentRequests.decrementAndGet();
      } else {
        queueSize.decrementAndGet();
        LOG.trace("[{}] Starting dequeued request", logPrefix);
        return nextRequest;
      }
    }

    // no next task was dequeued
    return null;
  }

  @Override
  public void close() {
    closed = true;

    LOG.debug("[{}] Rejecting {} queued requests after shutdown", logPrefix, queueSize.get());
    Throttled request;
    while ((request = queue.poll()) != null) {
      queueSize.decrementAndGet();
      fail(request, "The session is shutting down");
    }
  }

  public int getQueueSize() {
    return queueSize.get();
  }

  @VisibleForTesting
  int getConcurrentRequests() {
    return concurrentRequests.get();
  }

  @VisibleForTesting
  Deque<Throttled> getQueue() {
    return queue;
  }

  private static void fail(Throttled request, String message) {
    request.onThrottleFailure(new RequestThrottlingException(message));
  }
}
