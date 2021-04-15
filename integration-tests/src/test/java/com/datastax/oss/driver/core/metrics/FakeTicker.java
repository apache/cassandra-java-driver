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
package com.datastax.oss.driver.core.metrics;

import com.datastax.oss.driver.shaded.guava.common.base.Ticker;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/** A Ticker whose value can be advanced programmatically in test. */
public class FakeTicker extends Ticker {

  private final AtomicLong nanos = new AtomicLong();

  public FakeTicker advance(long nanoseconds) {
    nanos.addAndGet(nanoseconds);
    return this;
  }

  public FakeTicker advance(Duration duration) {
    return advance(duration.toNanos());
  }

  @Override
  public long read() {
    return nanos.get();
  }
}
