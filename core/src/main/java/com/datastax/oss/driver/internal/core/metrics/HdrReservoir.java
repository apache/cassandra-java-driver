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
package com.datastax.oss.driver.internal.core.metrics;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reservoir implementation backed by the HdrHistogram library.
 *
 * <p>It uses a {@link Recorder} to capture snapshots at a configurable interval: calls to {@link
 * #update(long)} are recorded in a "live" histogram, while {@link #getSnapshot()} is based on a
 * "cached", read-only histogram. Each time the cached histogram becomes older than the interval,
 * the two histograms are switched (therefore statistics won't be available during the first
 * interval after initialization, since we don't have a cached histogram yet).
 *
 * <p>Note that this class does not implement {@link #size()}.
 *
 * @see <a href="http://hdrhistogram.github.io/HdrHistogram/">HdrHistogram</a>
 */
@ThreadSafe
public class HdrReservoir implements Reservoir {

  private static final Logger LOG = LoggerFactory.getLogger(HdrReservoir.class);

  private final String logPrefix;
  private final Recorder recorder;
  private final long refreshIntervalNanos;

  // The lock only orchestrates `getSnapshot()` calls; `update()` is fed directly to the recorder,
  // which is lock-free. `getSnapshot()` calls are comparatively rare, so locking is not a
  // bottleneck.
  private final ReadWriteLock cacheLock = new ReentrantReadWriteLock();

  @GuardedBy("cacheLock")
  private Histogram cachedHistogram;

  @GuardedBy("cacheLock")
  private long cachedHistogramTimestampNanos;

  @GuardedBy("cacheLock")
  private Snapshot cachedSnapshot;

  public HdrReservoir(
      Duration highestTrackableLatency,
      int numberOfSignificantValueDigits,
      Duration refreshInterval,
      String logPrefix) {
    this.logPrefix = logPrefix;
    // The Reservoir interface is supposed to be agnostic to the unit. However, the Metrics library
    // heavily leans towards nanoseconds (for example, Timer feeds nanoseconds to update(); JmxTimer
    // assumes that the snapshot results are in nanoseconds).
    // In our case, microseconds are precise enough for request metrics, and we don't want to waste
    // space unnecessarily. So we simply use microseconds for our internal storage, and do the
    // conversion when needed.
    this.recorder =
        new Recorder(highestTrackableLatency.toNanos() / 1000, numberOfSignificantValueDigits);
    this.refreshIntervalNanos = refreshInterval.toNanos();
    this.cachedHistogramTimestampNanos = System.nanoTime();
    this.cachedSnapshot = EMPTY_SNAPSHOT;
  }

  @Override
  public void update(long value) {
    try {
      recorder.recordValue(value / 1000);
    } catch (ArrayIndexOutOfBoundsException e) {
      LOG.warn("[{}] Recorded value ({}) is out of bounds, discarding", logPrefix, value);
    }
  }

  /**
   * <em>Not implemented</em>: this reservoir implementation is intended for use with a {@link
   * com.codahale.metrics.Histogram}, which doesn't use this method.
   *
   * <p>(original description: {@inheritDoc})
   */
  @Override
  public int size() {
    throw new UnsupportedOperationException("HdrReservoir does not implement size()");
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note that the snapshots returned from this method do not implement {@link
   * Snapshot#getValues()} nor {@link Snapshot#dump(OutputStream)}. In addition, due to the way that
   * internal data structures are recycled, you should not hold onto a snapshot for more than the
   * refresh interval; one way to ensure this is to never cache the result of this method.
   */
  @Override
  public Snapshot getSnapshot() {
    long now = System.nanoTime();

    cacheLock.readLock().lock();
    try {
      if (now - cachedHistogramTimestampNanos < refreshIntervalNanos) {
        return cachedSnapshot;
      }
    } finally {
      cacheLock.readLock().unlock();
    }

    cacheLock.writeLock().lock();
    try {
      // Might have raced with another writer => re-check the timestamp
      if (now - cachedHistogramTimestampNanos >= refreshIntervalNanos) {
        LOG.debug("Cached snapshot is too old, refreshing");
        cachedHistogram = recorder.getIntervalHistogram(cachedHistogram);
        cachedSnapshot = new HdrSnapshot(cachedHistogram);
        cachedHistogramTimestampNanos = now;
      }
      return cachedSnapshot;
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  private class HdrSnapshot extends Snapshot {

    private final Histogram histogram;
    private final double meanNanos;
    private final double stdDevNanos;

    private HdrSnapshot(Histogram histogram) {
      this.histogram = histogram;

      // Cache those values because they rely on HdrHistogram's internal iterators, which are not
      // safe if the snapshot is accessed by concurrent reporters.
      // In contrast, getMin(), getMax() and getValue() are safe.
      this.meanNanos = histogram.getMean() * 1000;
      this.stdDevNanos = histogram.getStdDeviation() * 1000;
    }

    @Override
    public double getValue(double quantile) {
      return histogram.getValueAtPercentile(quantile * 100) * 1000;
    }

    /**
     * <em>Not implemented</em>: this reservoir implementation is intended for use with a {@link
     * com.codahale.metrics.Histogram}, which doesn't use this method.
     *
     * <p>(original description: {@inheritDoc})
     */
    @Override
    public long[] getValues() {
      // This can be implemented, but we ran into issues when accessed by concurrent reporters
      // because HdrHistogram uses an unsafe shared iterator.
      // So throwing instead since this method should be seldom used anyway.
      throw new UnsupportedOperationException(
          "HdrReservoir's snapshots do not implement getValues()");
    }

    @Override
    public int size() {
      long longSize = histogram.getTotalCount();
      // The Metrics API requires an int. It's very unlikely that we get an overflow here, unless
      // the refresh interval is ridiculously high (at 10k requests/s, it would have to be more than
      // 59 hours). However handle gracefully just in case.
      int size;
      if (longSize > Integer.MAX_VALUE) {
        LOG.warn("[{}] Too many recorded values, truncating", logPrefix);
        size = Integer.MAX_VALUE;
      } else {
        size = (int) longSize;
      }
      return size;
    }

    @Override
    public long getMax() {
      return histogram.getMaxValue() * 1000;
    }

    @Override
    public double getMean() {
      return meanNanos;
    }

    @Override
    public long getMin() {
      return histogram.getMinValue() * 1000;
    }

    @Override
    public double getStdDev() {
      return stdDevNanos;
    }

    /**
     * <em>Not implemented</em>: this reservoir implementation is intended for use with a {@link
     * com.codahale.metrics.Histogram}, which doesn't use this method.
     *
     * <p>(original description: {@inheritDoc})
     */
    @Override
    public void dump(OutputStream output) {
      throw new UnsupportedOperationException("HdrReservoir's snapshots do not implement dump()");
    }
  }

  private static final Snapshot EMPTY_SNAPSHOT =
      new Snapshot() {
        @Override
        public double getValue(double quantile) {
          return 0;
        }

        @Override
        public long[] getValues() {
          return new long[0];
        }

        @Override
        public int size() {
          return 0;
        }

        @Override
        public long getMax() {
          return 0;
        }

        @Override
        public double getMean() {
          return 0;
        }

        @Override
        public long getMin() {
          return 0;
        }

        @Override
        public double getStdDev() {
          return 0;
        }

        @Override
        public void dump(OutputStream output) {
          // nothing to do
        }
      };
}
