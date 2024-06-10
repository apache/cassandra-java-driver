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
package com.datastax.oss.driver.internal.core.util.concurrent;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;

/** Holds a reference to an object that is initialized on first access. */
@ThreadSafe
public class LazyReference<T> {

  private final String name;
  private final Supplier<T> supplier;
  private final CycleDetector checker;
  private volatile T value;
  private final ReentrantLock lock = new ReentrantLock();

  public LazyReference(String name, Supplier<T> supplier, CycleDetector cycleDetector) {
    this.name = name;
    this.supplier = supplier;
    this.checker = cycleDetector;
  }

  public LazyReference(Supplier<T> supplier) {
    this(null, supplier, null);
  }

  public T get() {
    T t = value;
    if (t == null) {
      if (checker != null) {
        checker.onTryLock(this);
      }
      lock.lock();
      try {
        if (checker != null) {
          checker.onLockAcquired(this);
        }
        t = value;
        if (t == null) {
          value = t = supplier.get();
        }
      } finally {
        if (checker != null) {
          checker.onReleaseLock(this);
        }
        lock.unlock();
      }
    }
    return t;
  }

  public String getName() {
    return name;
  }
}
