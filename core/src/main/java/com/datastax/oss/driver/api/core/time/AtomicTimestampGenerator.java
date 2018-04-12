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
package com.datastax.oss.driver.api.core.time;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.time.Clock;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicLong;
import net.jcip.annotations.ThreadSafe;

/**
 * A timestamp generator that guarantees monotonically increasing timestamps across all client
 * threads, and logs warnings when timestamps drift in the future.
 */
@ThreadSafe
public class AtomicTimestampGenerator extends MonotonicTimestampGenerator {

  private AtomicLong lastRef = new AtomicLong(0);

  public AtomicTimestampGenerator(DriverContext context) {
    super(context);
  }

  @VisibleForTesting
  AtomicTimestampGenerator(Clock clock, InternalDriverContext context) {
    super(clock, context);
  }

  @Override
  public long next() {
    while (true) {
      long last = lastRef.get();
      long next = computeNext(last);
      if (lastRef.compareAndSet(last, next)) {
        return next;
      }
    }
  }
}
