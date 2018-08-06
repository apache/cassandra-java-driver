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
package com.datastax.driver.core;

/**
 * Special exception that gets emitted to {@link LatencyTracker}s with the latencies of cancelled
 * speculative executions. This allows those trackers to choose whether to ignore those latencies or
 * not.
 */
class CancelledSpeculativeExecutionException extends Exception {

  static CancelledSpeculativeExecutionException INSTANCE =
      new CancelledSpeculativeExecutionException();

  private CancelledSpeculativeExecutionException() {
    super();
  }
}
