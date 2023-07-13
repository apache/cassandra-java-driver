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
package com.datastax.dse.driver.internal.core.tracker;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Combines multiple request trackers into a single one.
 *
 * <p>The default context always wraps any user-provided tracker into this, in case other internal
 * components need to add their own trackers later (see InternalDriverContext.buildRequestTracker).
 *
 * <p>We also use it to catch and log any unexpected exception thrown by a tracker.
 */
public class MultiplexingRequestTracker implements RequestTracker {

  private static final Logger LOG = LoggerFactory.getLogger(MultiplexingRequestTracker.class);

  private final List<RequestTracker> trackers = new CopyOnWriteArrayList<>();

  public void register(RequestTracker tracker) {
    trackers.add(tracker);
  }

  @Override
  public void onSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    for (RequestTracker tracker : trackers) {
      try {
        tracker.onSuccess(request, latencyNanos, executionProfile, node, logPrefix);
      } catch (Throwable t) {
        LOG.error("[{}] Unexpected error while invoking request tracker", logPrefix, t);
      }
    }
  }

  @Override
  public void onError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @Nullable Node node,
      @NonNull String logPrefix) {
    for (RequestTracker tracker : trackers) {
      try {
        tracker.onError(request, error, latencyNanos, executionProfile, node, logPrefix);
      } catch (Throwable t) {
        LOG.error("[{}] Unexpected error while invoking request tracker", logPrefix, t);
      }
    }
  }

  @Override
  public void onNodeSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    for (RequestTracker tracker : trackers) {
      try {
        tracker.onNodeSuccess(request, latencyNanos, executionProfile, node, logPrefix);
      } catch (Throwable t) {
        LOG.error("[{}] Unexpected error while invoking request tracker", logPrefix, t);
      }
    }
  }

  @Override
  public void onNodeError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    for (RequestTracker tracker : trackers) {
      try {
        tracker.onNodeError(request, error, latencyNanos, executionProfile, node, logPrefix);
      } catch (Throwable t) {
        LOG.error("[{}] Unexpected error while invoking request tracker", logPrefix, t);
      }
    }
  }

  @Override
  public void onSessionReady(@NonNull Session session) {
    for (RequestTracker tracker : trackers) {
      try {
        tracker.onSessionReady(session);
      } catch (Throwable t) {
        LOG.error("[{}] Unexpected error while invoking request tracker", session.getName(), t);
      }
    }
  }

  @Override
  public void close() throws Exception {
    Exception toThrow = null;
    for (RequestTracker tracker : trackers) {
      try {
        tracker.close();
      } catch (Exception e) {
        if (toThrow == null) {
          toThrow = e;
        } else {
          toThrow.addSuppressed(e);
        }
      }
    }
    if (toThrow != null) {
      throw toThrow;
    }
  }
}
