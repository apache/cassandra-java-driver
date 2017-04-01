/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.util.concurrent;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Holds a reference to an object that is initialized on first access. */
public class LazyReference<T> {
  private static final Logger LOG = LoggerFactory.getLogger(LazyReference.class);

  private final String name;
  private final Supplier<T> supplier;
  private volatile T value;
  private ReentrantLock lock;

  public LazyReference(String name, Supplier<T> supplier) {
    this.name = name;
    this.supplier = supplier;
  }

  public T get() {
    T t = value;
    if (t == null) {
      lock.lock();
      try {
        t = value;
        if (t == null) {
          LOG.debug("Initializing {}", name);
          value = t = supplier.get();
        }
      } finally {
        lock.unlock();
      }
    }
    return t;
  }
}
