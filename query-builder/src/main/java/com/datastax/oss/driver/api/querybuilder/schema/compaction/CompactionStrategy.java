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

import com.datastax.oss.driver.api.querybuilder.schema.OptionProvider;
import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface CompactionStrategy<SelfT extends CompactionStrategy<SelfT>>
    extends OptionProvider<SelfT> {

  @NonNull
  @CheckReturnValue
  default SelfT withEnabled(boolean enabled) {
    return withOption("enabled", enabled);
  }

  @NonNull
  @CheckReturnValue
  default SelfT withTombstoneCompactionIntervalInSeconds(int seconds) {
    return withOption("tombstone_compaction_interval", seconds);
  }

  @NonNull
  @CheckReturnValue
  default SelfT withTombstoneThreshold(double threshold) {
    return withOption("tombstone_threshold", threshold);
  }

  @NonNull
  @CheckReturnValue
  default SelfT withUncheckedTombstoneCompaction(boolean enabled) {
    return withOption("unchecked_tombstone_compaction", enabled);
  }
}
