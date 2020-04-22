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
package com.datastax.oss.driver.api.core.metadata.diagnostic;

/** The status of a {@link Diagnostic}. */
public enum Status {

  /** The diagnosed system is fully available. */
  AVAILABLE(0),

  /**
   * The diagnosed system is partially available and functions in degraded mode, but is still
   * capable of fulfilling its purpose.
   */
  PARTIALLY_AVAILABLE(1),

  /** The diagnosed system is not available at all; it cannot fulfill its purpose. */
  UNAVAILABLE(2),

  /** The diagnosed system is in an unknown state. */
  UNKNOWN(3);

  private final int precedence;

  Status(int precedence) {
    this.precedence = precedence;
  }

  /**
   * Merges the given {@link Status} with this one, in a pessimistic way: unavailable statuses take
   * precedence over available ones.
   */
  public Status mergeWith(Status that) {
    return this.precedence >= that.precedence ? this : that;
  }
}
