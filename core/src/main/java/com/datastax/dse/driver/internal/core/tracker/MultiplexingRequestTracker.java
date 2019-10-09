/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.tracker;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class MultiplexingRequestTracker implements RequestTracker {

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
      tracker.onSuccess(request, latencyNanos, executionProfile, node, logPrefix);
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
      tracker.onError(request, error, latencyNanos, executionProfile, node, logPrefix);
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
      tracker.onNodeSuccess(request, latencyNanos, executionProfile, node, logPrefix);
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
      tracker.onNodeError(request, error, latencyNanos, executionProfile, node, logPrefix);
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
