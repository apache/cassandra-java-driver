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
package com.datastax.oss.driver.api.core.servererrors;

/** A default write type supported by the driver out of the box. */
public enum DefaultWriteType implements WriteType {

  /** A write to a single partition key. Such writes are guaranteed to be atomic and isolated. */
  SIMPLE,
  /**
   * A write to a multiple partition key that used the distributed batch log to ensure atomicity
   * (atomicity meaning that if any statement in the batch succeeds, all will eventually succeed).
   */
  BATCH,
  /**
   * A write to a multiple partition key that doesn't use the distributed batch log. Atomicity for
   * such writes is not guaranteed
   */
  UNLOGGED_BATCH,
  /**
   * A counter write (that can be for one or multiple partition key). Such write should not be
   * replayed to avoid over-counting.
   */
  COUNTER,
  /**
   * The initial write to the distributed batch log that Cassandra performs internally before a
   * BATCH write.
   */
  BATCH_LOG,
  /**
   * A conditional write. If a timeout has this {@code WriteType}, the timeout has happened while
   * doing the compare-and-swap for an conditional update. In this case, the update may or may not
   * have been applied.
   */
  CAS,
  /**
   * Indicates that the timeout was related to acquiring locks needed for updating materialized
   * views affected by write operation.
   */
  VIEW,
  /**
   * Indicates that the timeout was related to acquiring space for change data capture logs for cdc
   * tracked tables.
   */
  CDC,
  ;
  // Note that, for the sake of convenience, we also expose shortcuts to these constants on the
  // WriteType interface. If you add a new enum constant, remember to update the interface as
  // well.
}
