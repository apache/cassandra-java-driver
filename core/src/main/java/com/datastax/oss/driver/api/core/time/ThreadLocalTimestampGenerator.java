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

import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.time.Clock;
import com.google.common.annotations.VisibleForTesting;

/**
 * A timestamp generator that guarantees monotonically increasing timestamps within each thread, and
 * logs warnings when timestamps drift in the future.
 *
 * <p>Beware that there is a risk of timestamp collision with this generator when accessed by more
 * than one thread at a time; only use it when threads are not in direct competition for timestamp
 * ties (i.e., they are executing independent statements).
 */
public class ThreadLocalTimestampGenerator extends MonotonicTimestampGenerator {

  private final ThreadLocal<Long> lastRef = ThreadLocal.withInitial(() -> 0L);

  public ThreadLocalTimestampGenerator(DriverContext context, DriverOption configRoot) {
    super(context, configRoot);
  }

  @VisibleForTesting
  ThreadLocalTimestampGenerator(Clock clock, DriverContext context, DriverOption configRoot) {
    super(clock, context, configRoot);
  }

  @Override
  public long next() {
    Long last = this.lastRef.get();
    long next = computeNext(last);
    this.lastRef.set(next);
    return next;
  }
}
