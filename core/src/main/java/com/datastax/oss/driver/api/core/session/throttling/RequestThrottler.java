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
package com.datastax.oss.driver.api.core.session.throttling;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.Closeable;

/**
 * Limits the number of concurrent requests executed by the driver.
 *
 * <p>Usage in non-blocking applications: beware that all built-in implementations of this interface
 * use locks for internal coordination, and do not qualify as lock-free, with the obvious exception
 * of {@code PassThroughRequestThrottler}. If your application enforces strict lock-freedom, then
 * request throttling should not be enabled.
 */
public interface RequestThrottler extends Closeable {

  /**
   * Registers a new request to be throttled. The throttler will invoke {@link
   * Throttled#onThrottleReady(boolean)} when the request is allowed to proceed.
   */
  void register(@NonNull Throttled request);

  /**
   * Signals that a request has succeeded. This indicates to the throttler that another request
   * might be started.
   */
  void signalSuccess(@NonNull Throttled request);

  /**
   * Signals that a request has failed. This indicates to the throttler that another request might
   * be started.
   */
  void signalError(@NonNull Throttled request, @NonNull Throwable error);

  /**
   * Signals that a request has timed out. This indicates to the throttler that this request has
   * stopped (if it was running already), or that it doesn't need to be started in the future.
   *
   * <p>Note: requests are responsible for handling their own timeout. The throttler does not
   * perform time-based eviction on pending requests.
   */
  void signalTimeout(@NonNull Throttled request);
}
