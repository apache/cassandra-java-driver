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
package com.datastax.oss.driver.internal.core.pool;

import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterators;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;
import net.jcip.annotations.ThreadSafe;

/**
 * Concurrent structure used to store the channels of a pool.
 *
 * <p>Its write semantics are similar to "copy-on-write" JDK collections, selection operations are
 * expected to vastly outnumber mutations.
 */
@ThreadSafe
class ChannelSet implements Iterable<DriverChannel> {
  private volatile DriverChannel[] channels;
  private ReentrantLock lock = new ReentrantLock(); // must be held when mutating the array

  ChannelSet() {
    this.channels = new DriverChannel[] {};
  }

  void add(DriverChannel toAdd) {
    Preconditions.checkNotNull(toAdd);
    lock.lock();
    try {
      assert indexOf(channels, toAdd) < 0;
      DriverChannel[] newChannels = Arrays.copyOf(channels, channels.length + 1);
      newChannels[newChannels.length - 1] = toAdd;
      channels = newChannels;
    } finally {
      lock.unlock();
    }
  }

  boolean remove(DriverChannel toRemove) {
    Preconditions.checkNotNull(toRemove);
    lock.lock();
    try {
      int index = indexOf(channels, toRemove);
      if (index < 0) {
        return false;
      } else {
        DriverChannel[] newChannels = new DriverChannel[channels.length - 1];
        int newI = 0;
        for (int i = 0; i < channels.length; i++) {
          if (i != index) {
            newChannels[newI] = channels[i];
            newI += 1;
          }
        }
        channels = newChannels;
        return true;
      }
    } finally {
      lock.unlock();
    }
  }

  /** @return null if the set is empty or all are full */
  DriverChannel next() {
    DriverChannel[] snapshot = this.channels;
    switch (snapshot.length) {
      case 0:
        return null;
      case 1:
        return snapshot[0];
      default:
        DriverChannel best = null;
        int bestScore = 0;
        for (DriverChannel channel : snapshot) {
          int score = channel.getAvailableIds();
          if (score > bestScore) {
            bestScore = score;
            best = channel;
          }
        }
        return best;
    }
  }

  /** @return the number of available stream ids on all channels in this channel set. */
  int getAvailableIds() {
    int availableIds = 0;
    DriverChannel[] snapshot = this.channels;
    for (DriverChannel channel : snapshot) {
      availableIds += channel.getAvailableIds();
    }
    return availableIds;
  }

  /**
   * @return the number of requests currently executing on all channels in this channel set
   *     (including {@link #getOrphanedIds() orphaned ids}).
   */
  int getInFlight() {
    int inFlight = 0;
    DriverChannel[] snapshot = this.channels;
    for (DriverChannel channel : snapshot) {
      inFlight += channel.getInFlight();
    }
    return inFlight;
  }

  /**
   * @return the number of stream ids for requests in all channels in this channel set that have
   *     either timed out or been cancelled, but for which we can't release the stream id because a
   *     request might still come from the server.
   */
  int getOrphanedIds() {
    int orphanedIds = 0;
    DriverChannel[] snapshot = this.channels;
    for (DriverChannel channel : snapshot) {
      orphanedIds += channel.getOrphanedIds();
    }
    return orphanedIds;
  }

  int size() {
    return this.channels.length;
  }

  @NonNull
  @Override
  public Iterator<DriverChannel> iterator() {
    return Iterators.forArray(this.channels);
  }

  private static int indexOf(DriverChannel[] channels, DriverChannel key) {
    for (int i = 0; i < channels.length; i++) {
      if (channels[i] == key) {
        return i;
      }
    }
    return -1;
  }
}
