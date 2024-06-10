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
package com.datastax.dse.driver.internal.core.cql.reactive;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.atomic.AtomicLong;

public final class ReactiveOperators {

  /**
   * Atomically adds the given value to the given AtomicLong, bound to Long.MAX_VALUE.
   *
   * @param current the current value.
   * @param toAdd the delta to add.
   */
  public static void addCap(@NonNull AtomicLong current, long toAdd) {
    long r, u;
    do {
      r = current.get();
      if (r == Long.MAX_VALUE) {
        return;
      }
      u = r + toAdd;
      if (u < 0L) {
        u = Long.MAX_VALUE;
      }
    } while (!current.compareAndSet(r, u));
  }

  /**
   * Atomically subtracts the given value from the given AtomicLong, bound to 0.
   *
   * @param current the current value.
   * @param toSub the delta to subtract.
   */
  public static void subCap(@NonNull AtomicLong current, long toSub) {
    long r, u;
    do {
      r = current.get();
      if (r == 0 || r == Long.MAX_VALUE) {
        return;
      }
      u = Math.max(r - toSub, 0);
    } while (!current.compareAndSet(r, u));
  }
}
