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
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.NotThreadSafe;

/**
 * Manages the set of identifiers used to distinguish multiplexed requests on a channel.
 *
 * <p>{@link #preAcquire()} / {@link #getAvailableIds()} follow atomic semantics. See {@link
 * DriverChannel#preAcquireId()} for more explanations.
 *
 * <p>Other methods are not synchronized, they are only called by {@link InFlightHandler} on the I/O
 * thread.
 */
@NotThreadSafe
class StreamIdGenerator {

  private final int maxAvailableIds;
  // unset = available, set = borrowed (note that this is the opposite of the 3.x implementation)
  private final BitSet ids;
  private final AtomicInteger availableIds;

  StreamIdGenerator(int maxAvailableIds) {
    this.maxAvailableIds = maxAvailableIds;
    this.ids = new BitSet(this.maxAvailableIds);
    this.availableIds = new AtomicInteger(this.maxAvailableIds);
  }

  boolean preAcquire() {
    while (true) {
      int current = availableIds.get();
      assert current >= 0;
      if (current == 0) {
        return false;
      } else if (availableIds.compareAndSet(current, current - 1)) {
        return true;
      }
    }
  }

  void cancelPreAcquire() {
    int available = availableIds.incrementAndGet();
    assert available <= maxAvailableIds;
  }

  int acquire() {
    assert availableIds.get() < maxAvailableIds;
    int id = ids.nextClearBit(0);
    if (id >= maxAvailableIds) {
      return -1;
    }
    ids.set(id);
    return id;
  }

  void release(int id) {
    if (!ids.get(id)) {
      throw new IllegalStateException("Tried to release id that hadn't been borrowed: " + id);
    }
    ids.clear(id);
    int available = availableIds.incrementAndGet();
    assert available <= maxAvailableIds;
  }

  int getAvailableIds() {
    return availableIds.get();
  }

  int getMaxAvailableIds() {
    return maxAvailableIds;
  }
}
