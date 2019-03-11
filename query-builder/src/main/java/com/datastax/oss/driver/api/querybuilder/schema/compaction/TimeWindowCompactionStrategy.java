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

public interface TimeWindowCompactionStrategy<SelfT extends TimeWindowCompactionStrategy<SelfT>>
    extends CompactionStrategy<SelfT>, SizeTieredCompactionStrategy<SelfT> {

  enum CompactionWindowUnit {
    MINUTES,
    HOURS,
    DAYS
  }

  enum TimestampResolution {
    MICROSECONDS,
    MILLISECONDS
  }

  @NonNull
  @CheckReturnValue
  default SelfT withCompactionWindow(long size, @NonNull CompactionWindowUnit unit) {
    return withOption("compaction_window_size", size)
        .withOption("compaction_window_unit", unit.toString());
  }

  @NonNull
  @CheckReturnValue
  default SelfT withUnsafeAggressiveSSTableExpiration(boolean enabled) {
    return withOption("unsafe_aggressive_sstable_expiration", enabled);
  }

  @NonNull
  @CheckReturnValue
  default SelfT withTimestampResolution(@NonNull TimestampResolution timestampResolution) {
    return withOption("timestamp_resolution", timestampResolution.toString());
  }
}
