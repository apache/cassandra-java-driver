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
package com.datastax.oss.driver.internal.core.channel;

import java.util.BitSet;

/**
 * Manages the set of identifiers used to distinguish multiplexed requests on a channel.
 *
 * <p>This class is not thread safe: calls to {@link #acquire()} and {@link #release(int)} must be
 * properly synchronized (in practice this is done by only calling them from the I/O thread).
 * However, {@link #getAvailableIds()} has volatile semantics.
 */
class StreamIdGenerator {

  private final int maxAvailableIds;
  // unset = available, set = borrowed (note that this is the opposite of the 3.x implementation)
  private final BitSet ids;
  private volatile int availableIds;

  StreamIdGenerator(int maxAvailableIds) {
    this.maxAvailableIds = maxAvailableIds;
    this.ids = new BitSet(this.maxAvailableIds);
    this.availableIds = this.maxAvailableIds;
  }

  int acquire() {
    int id = ids.nextClearBit(0);
    if (id >= maxAvailableIds) {
      return -1;
    }
    ids.set(id);
    availableIds--;
    return id;
  }

  void release(int id) {
    if (ids.get(id)) {
      availableIds++;
    } else {
      throw new IllegalStateException("Tried to release id that hadn't been borrowed: " + id);
    }
    ids.clear(id);
  }

  int getAvailableIds() {
    return availableIds;
  }

  int getMaxAvailableIds() {
    return maxAvailableIds;
  }
}
