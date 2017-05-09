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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * Filters a list of events, accumulating them during an initialization period.
 *
 * <p>It has three states:
 *
 * <ul>
 *   <li>Not started: events are discarded.
 *   <li>Started: events accumulate but are not propagated to the end consumer yet.
 *   <li>Ready: all accumulated events are flushed to the end consumer; subsequent events are
 *       propagated directly. The order of events is preserved at all times.
 * </ul>
 */
public class ReplayingEventFilter<T> {

  private enum State {
    NEW,
    STARTED,
    READY
  }

  private final Consumer<T> consumer;

  // Exceptionally, we use a lock: it will rarely be contended, and if so for only a short period.
  private final ReadWriteLock stateLock = new ReentrantReadWriteLock();
  private State state;
  private List<T> recordedEvents;

  public ReplayingEventFilter(Consumer<T> consumer) {
    this.consumer = consumer;
    this.state = State.NEW;
    this.recordedEvents = new CopyOnWriteArrayList<>();
  }

  public void start() {
    stateLock.writeLock().lock();
    try {
      state = State.STARTED;
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  public void markReady() {
    stateLock.writeLock().lock();
    try {
      state = State.READY;
      for (T event : recordedEvents) {
        consumer.accept(event);
      }
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  public void accept(T event) {
    stateLock.readLock().lock();
    try {
      switch (state) {
        case NEW:
          break;
        case STARTED:
          recordedEvents.add(event);
          break;
        case READY:
          consumer.accept(event);
          break;
      }
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @VisibleForTesting
  public List<T> recordedEvents() {
    stateLock.readLock().lock();
    try {
      return ImmutableList.copyOf(recordedEvents);
    } finally {
      stateLock.readLock().unlock();
    }
  }
}
