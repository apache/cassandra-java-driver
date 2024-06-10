/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.time;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import net.jcip.annotations.ThreadSafe;

/**
 * A timestamp generator that guarantees monotonically increasing timestamps within each thread, and
 * logs warnings when timestamps drift in the future.
 *
 * <p>Beware that there is a risk of timestamp collision with this generator when accessed by more
 * than one thread at a time; only use it when threads are not in direct competition for timestamp
 * ties (i.e., they are executing independent statements).
 *
 * <p>To activate this generator, modify the {@code advanced.timestamp-generator} section in the
 * driver configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.timestamp-generator {
 *     class = ThreadLocalTimestampGenerator
 *     drift-warning {
 *       threshold = 1 second
 *       interval = 10 seconds
 *     }
 *     force-java-clock = false
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
@ThreadSafe
public class ThreadLocalTimestampGenerator extends MonotonicTimestampGenerator {

  private final ThreadLocal<Long> lastRef = ThreadLocal.withInitial(() -> 0L);

  public ThreadLocalTimestampGenerator(DriverContext context) {
    super(context);
  }

  @VisibleForTesting
  ThreadLocalTimestampGenerator(Clock clock, DriverContext context) {
    super(clock, context);
  }

  @Override
  public long next() {
    Long last = this.lastRef.get();
    long next = computeNext(last);
    this.lastRef.set(next);
    return next;
  }
}
