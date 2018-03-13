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
package com.datastax.oss.driver.internal.core.session.throttling;

class SettableNanoClock implements NanoClock {

  private volatile long nanoTime;

  @Override
  public long nanoTime() {
    return nanoTime;
  }

  // This is racy, but in our tests it's never read concurrently
  @SuppressWarnings("NonAtomicVolatileUpdate")
  void add(long increment) {
    nanoTime += increment;
  }
}
