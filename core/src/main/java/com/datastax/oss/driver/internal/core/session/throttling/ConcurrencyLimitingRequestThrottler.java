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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.ReentrantLock;
import net.jcip.annotations.GuardedBy;
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

  private final ReentrantLock lock = new ReentrantLock();

  @GuardedBy("lock")
  private int concurrentRequests;

  @GuardedBy("lock")
  private final Deque<Throttled> queue = new ArrayDeque<>();

  @GuardedBy("lock")
  private boolean closed;

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
    boolean notifyReadyRequired = false;

    lock.lock();
    try {
      if (closed) {
        LOG.trace("[{}] Rejecting request after shutdown", logPrefix);
        fail(request, "The session is shutting down");
      } else if (queue.isEmpty() && concurrentRequests < maxConcurrentRequests) {
        // We have capacity for one more concurrent request
        LOG.trace("[{}] Starting newly registered request", logPrefix);
        concurrentRequests += 1;
        notifyReadyRequired = true;
      } else if (queue.size() < maxQueueSize) {
        LOG.trace("[{}] Enqueuing request", logPrefix);
        queue.add(request);
      } else {
        LOG.trace("[{}] Rejecting request because of full queue", logPrefix);
        fail(
            request,
            String.format(
                "The session has reached its maximum capacity "
                    + "(concurrent requests: %d, queue size: %d)",
                maxConcurrentRequests, maxQueueSize));
      }
    } finally {
      lock.unlock();
    }

    // no need to hold the lock while allowing the task to progress
    if (notifyReadyRequired) {
      request.onThrottleReady(false);
    }
  }

  @Override
  public void signalSuccess(@NonNull Throttled request) {
    Throttled nextRequest = null;
    lock.lock();
    try {
      nextRequest = onRequestDoneAndDequeNext();
    } finally {
      lock.unlock();
    }

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
    lock.lock();
    try {
      if (!closed) {
        if (queue.remove(request)) { // The request timed out before it was active
          LOG.trace("[{}] Removing timed out request from the queue", logPrefix);
        } else {
          nextRequest = onRequestDoneAndDequeNext();
        }
      }
    } finally {
      lock.unlock();
    }

    if (nextRequest != null) {
      nextRequest.onThrottleReady(true);
    }
  }

  @Override
  public void signalCancel(@NonNull Throttled request) {
    Throttled nextRequest = null;
    lock.lock();
    try {
      if (!closed) {
        if (queue.remove(request)) { // The request has been cancelled before it was active
          LOG.trace("[{}] Removing cancelled request from the queue", logPrefix);
        } else {
          nextRequest = onRequestDoneAndDequeNext();
        }
      }
    } finally {
      lock.unlock();
    }

    if (nextRequest != null) {
      nextRequest.onThrottleReady(true);
    }
  }

  @SuppressWarnings("GuardedBy") // this method is only called with the lock held
  @Nullable
  private Throttled onRequestDoneAndDequeNext() {
    assert lock.isHeldByCurrentThread();
    if (!closed) {
      if (queue.isEmpty()) {
        concurrentRequests -= 1;
      } else {
        LOG.trace("[{}] Starting dequeued request", logPrefix);
        // don't touch concurrentRequests since we finished one but started another
        return queue.poll();
      }
    }

    // no next task was dequeued
    return null;
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

  public int getQueueSize() {
    lock.lock();
    try {
      return queue.size();
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  int getConcurrentRequests() {
    lock.lock();
    try {
      return concurrentRequests;
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
