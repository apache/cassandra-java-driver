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

import com.datastax.oss.driver.api.core.RequestThrottlingException;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A request that may be subjected to throttling by a {@link
 * com.datastax.oss.driver.api.core.session.throttling.RequestThrottler}.
 */
public interface Throttled {

  /**
   * Invoked by the throttler to indicate that the request can now start. The request must wait for
   * this call until it does any "actual" work (typically, writing to a connection).
   *
   * @param wasDelayed indicates whether the throttler delayed at all; this is so that requests
   *     don't have to rely on measuring time to determine it (this is useful for metrics).
   */
  void onThrottleReady(boolean wasDelayed);

  /**
   * Invoked by the throttler to indicate that the request cannot be fulfilled. Typically, this
   * means we've reached maximum capacity, and the request can't even be enqueued. This error must
   * be rethrown to the client.
   *
   * @param error the error that the request should be completed (exceptionally) with.
   */
  void onThrottleFailure(@NonNull RequestThrottlingException error);
}
