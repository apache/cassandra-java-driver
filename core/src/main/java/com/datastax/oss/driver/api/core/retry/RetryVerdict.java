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
package com.datastax.oss.driver.api.core.retry;

import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryVerdict;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The verdict returned by a {@link RetryPolicy} determining what to do when a request failed. A
 * verdict contains a {@link RetryDecision} indicating if a retry should be attempted at all and
 * where, and a method that allows the original request to be modified before the retry.
 */
@FunctionalInterface
public interface RetryVerdict {

  /** A retry verdict that retries the same request on the same node. */
  RetryVerdict RETRY_SAME = new DefaultRetryVerdict(RetryDecision.RETRY_SAME);

  /** A retry verdict that retries the same request on the next node in the query plan. */
  RetryVerdict RETRY_NEXT = new DefaultRetryVerdict(RetryDecision.RETRY_NEXT);

  /** A retry verdict that ignores the error, returning and empty result set to the caller. */
  RetryVerdict IGNORE = new DefaultRetryVerdict(RetryDecision.IGNORE);

  /** A retry verdict that rethrows the execution error to the calling code. */
  RetryVerdict RETHROW = new DefaultRetryVerdict(RetryDecision.RETHROW);

  /** @return The retry decision to apply. */
  @NonNull
  RetryDecision getRetryDecision();

  /**
   * Returns the request to retry, based on the request that was just executed (and failed).
   *
   * <p>The default retry policy always returns the request as is. Custom retry policies can use
   * this method to customize the request to retry, for example, by changing its consistency level,
   * query timestamp, custom payload, or even its execution profile.
   *
   * @param <RequestT> The actual type of the request.
   * @param previous The request that was just executed (and failed).
   * @return The request to retry.
   */
  @NonNull
  default <RequestT extends Request> RequestT getRetryRequest(@NonNull RequestT previous) {
    return previous;
  }
}
