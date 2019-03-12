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
package com.datastax.oss.driver.api.querybuilder.schema.compaction;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface SizeTieredCompactionStrategy<SelfT extends SizeTieredCompactionStrategy<SelfT>>
    extends CompactionStrategy<SelfT> {

  @NonNull
  @CheckReturnValue
  default SelfT withMaxThreshold(int maxThreshold) {
    return withOption("max_threshold", maxThreshold);
  }

  @NonNull
  @CheckReturnValue
  default SelfT withMinThreshold(int minThreshold) {
    return withOption("min_threshold", minThreshold);
  }

  @NonNull
  @CheckReturnValue
  default SelfT withMinSSTableSizeInBytes(long bytes) {
    return withOption("min_sstable_size", bytes);
  }

  @NonNull
  @CheckReturnValue
  default SelfT withOnlyPurgeRepairedTombstones(boolean enabled) {
    return withOption("only_purge_repaired_tombstones", enabled);
  }

  @NonNull
  @CheckReturnValue
  default SelfT withBucketHigh(double bucketHigh) {
    return withOption("bucket_high", bucketHigh);
  }

  @NonNull
  @CheckReturnValue
  default SelfT withBucketLow(double bucketHigh) {
    return withOption("bucket_low", bucketHigh);
  }

  // 2.1 only
  @NonNull
  @CheckReturnValue
  default SelfT withColdReadsToOmit(double ratio) {
    return withOption("cold_reads_to_omit", ratio);
  }
}
