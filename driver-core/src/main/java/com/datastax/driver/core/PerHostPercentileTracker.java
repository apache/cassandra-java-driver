/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.*;
import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.*;

/**
 * A {@link LatencyTracker} that records latencies for each host over a sliding time interval, and exposes an
 * API to retrieve the latency at a given percentile.
 * <p/>
 * To use this class, build an instance with {@link #builderWithHighestTrackableLatencyMillis(long)} and register
 * it with your {@link com.datastax.driver.core.Cluster} instance:
 * <pre>
 * PerHostPercentileTracker tracker = PerHostPercentileTracker
 *     .builderWithHighestTrackableLatencyMillis(15000)
 *     .build();
 *
 * cluster.register(tracker);
 * ...
 * tracker.getLatencyAtPercentile(host1, 99.0);
 * </pre>
 * <p/>
 * This class uses <a href="http://hdrhistogram.github.io/HdrHistogram/">HdrHistogram</a> to record latencies:
 * for each host, there is a "live" histogram where current latencies are recorded, and a "cached", read-only histogram
 * that is used when clients call {@link #getLatencyAtPercentile(Host, double)}. Each time the cached histogram becomes
 * older than the interval, the two histograms are switched. Note that statistics will not be available during the first
 * interval at cluster startup, since we don't have a cached histogram yet.
 * <p/>
 * Note that this class is currently marked "beta": it hasn't been extensively tested yet, and the API is still subject
 * to change.
 */
@Beta
public class PerHostPercentileTracker implements LatencyTracker {
    private static final Logger logger = LoggerFactory.getLogger(PerHostPercentileTracker.class);

    private final ConcurrentMap<Host, Recorder> recorders;
    private final ConcurrentMap<Host, CachedHistogram> cachedHistograms;
    private final long highestTrackableLatencyMillis;
    private final int numberOfSignificantValueDigits;
    private final int minRecordedValues;
    private final long intervalMs;

    private PerHostPercentileTracker(long highestTrackableLatencyMillis, int numberOfSignificantValueDigits,
                                     int numberOfHosts,
                                     int minRecordedValues,
                                     long intervalMs) {
        this.highestTrackableLatencyMillis = highestTrackableLatencyMillis;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
        this.minRecordedValues = minRecordedValues;
        this.intervalMs = intervalMs;
        this.recorders = new MapMaker().initialCapacity(numberOfHosts).makeMap();
        this.cachedHistograms = new MapMaker().initialCapacity(numberOfHosts).makeMap();
    }

    /**
     * Returns a builder to create a new instance.
     *
     * @param highestTrackableLatencyMillis the highest expected latency. If a higher value is reported, it will be ignored and a
     *                                      warning will be logged. A good rule of thumb is to set it slightly higher than
     *                                      {@link SocketOptions#getReadTimeoutMillis()}.
     * @return the builder.
     */
    public static Builder builderWithHighestTrackableLatencyMillis(long highestTrackableLatencyMillis) {
        return new Builder(highestTrackableLatencyMillis);
    }

    /**
     * Helper class to builder {@code PerHostPercentileTracker} instances with a fluent interface.
     */
    public static class Builder {
        private final long highestTrackableLatencyMillis;
        private int numberOfSignificantValueDigits = 3;
        private int minRecordedValues = 1000;
        private int numberOfHosts = 16;
        private long intervalMs = MINUTES.toMillis(5);

        Builder(long highestTrackableLatencyMillis) {
            this.highestTrackableLatencyMillis = highestTrackableLatencyMillis;
        }

        /**
         * Sets the number of significant decimal digits to which histograms will maintain value
         * resolution and separation. This must be an integer between 0 and 5.
         * <p/>
         * If not set explicitly, this value defaults to 3.
         * <p/>
         * See <a href="http://hdrhistogram.github.io/HdrHistogram/JavaDoc/org/HdrHistogram/Histogram.html">the HdrHistogram Javadocs</a>
         * for a more detailed explanation on how this parameter affects the resolution of recorded samples.
         *
         * @param numberOfSignificantValueDigits the new value.
         * @return this builder.
         */
        public Builder withNumberOfSignificantValueDigits(int numberOfSignificantValueDigits) {
            this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
            return this;
        }

        /**
         * Sets the minimum number of values that must be recorded for a host before we consider
         * the sample size significant.
         * <p/>
         * If this count is not reached during a given interval, {@link #getLatencyAtPercentile(Host, double)}
         * will return a negative value, indicating that statistics are not available. In particular, this is true
         * during the first interval.
         * <p/>
         * If not set explicitly, this value default to 1000.
         *
         * @param minRecordedValues the new value.
         * @return this builder.
         */
        public Builder withMinRecordedValues(int minRecordedValues) {
            this.minRecordedValues = minRecordedValues;
            return this;
        }

        /**
         * Sets the number of distinct hosts that the driver will ever connect to.
         * <p/>
         * This parameter is only used to pre-size internal maps in order to avoid unnecessary rehashing.
         * <p/>
         * If not set explicitly, this value defaults to 16.
         *
         * @param numberOfHosts the new value.
         * @return this builder.
         */
        public Builder withNumberOfHosts(int numberOfHosts) {
            this.numberOfHosts = numberOfHosts;
            return this;
        }

        /**
         * Sets the time interval over which samples are recorded.
         * <p/>
         * For each host, there is a "live" histogram where current latencies are recorded, and a "cached", read-only histogram
         * that is used when clients call {@link #getLatencyAtPercentile(Host, double)}. Each time the cached histogram becomes
         * older than the interval, the two histograms are switched. Note that statistics will not be available during the first
         * interval at cluster startup, since we don't have a cached histogram yet.
         * <p/>
         * If not set explicitly, this value defaults to 5 minutes.
         *
         * @param interval the new interval.
         * @param unit     the unit that the interval is expressed in.
         * @return this builder.
         */
        public Builder withInterval(long interval, TimeUnit unit) {
            this.intervalMs = MILLISECONDS.convert(interval, unit);
            return this;
        }

        /**
         * Builds the {@code PerHostPercentileTracker} instance configured with this builder.
         *
         * @return the instance.
         */
        public PerHostPercentileTracker build() {
            return new PerHostPercentileTracker(highestTrackableLatencyMillis, numberOfSignificantValueDigits, numberOfHosts, minRecordedValues, intervalMs);
        }
    }

    @Override
    public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
        if (!shouldConsiderNewLatency(statement, exception))
            return;

        long latencyMs = NANOSECONDS.toMillis(newLatencyNanos);
        try {
            getRecorder(host).recordValue(latencyMs);
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.warn("Got request with latency of {} ms, which exceeds the configured maximum trackable value {}",
                    latencyMs, highestTrackableLatencyMillis);
        }
    }

    /**
     * Returns the request latency for a host at a given percentile.
     *
     * @param host       the host.
     * @param percentile the percentile (for example, {@code 99.0} for the 99th percentile).
     * @return the latency (in milliseconds) at the given percentile, or a negative value if it's not available yet.
     */
    public long getLatencyAtPercentile(Host host, double percentile) {
        checkArgument(percentile >= 0.0 && percentile < 100,
                "percentile must be between 0.0 and 100 (was %f)");
        Histogram histogram = getLastIntervalHistogram(host);
        if (histogram == null || histogram.getTotalCount() < minRecordedValues)
            return -1;

        return histogram.getValueAtPercentile(percentile);
    }

    private Recorder getRecorder(Host host) {
        Recorder recorder = recorders.get(host);
        if (recorder == null) {
            recorder = new Recorder(highestTrackableLatencyMillis, numberOfSignificantValueDigits);
            Recorder old = recorders.putIfAbsent(host, recorder);
            if (old != null) {
                // We got beaten at creating the recorder, use the actual instance and discard ours
                recorder = old;
            } else {
                // Also set an empty cache entry to remember the time we started recording:
                cachedHistograms.putIfAbsent(host, CachedHistogram.empty());
            }
        }
        return recorder;
    }

    /**
     * @return null if no histogram is available yet (no entries recorded, or not for long enough)
     */
    private Histogram getLastIntervalHistogram(Host host) {
        try {
            while (true) {
                CachedHistogram entry = cachedHistograms.get(host);
                if (entry == null)
                    return null;

                long age = System.currentTimeMillis() - entry.timestamp;
                if (age < intervalMs) { // current histogram is recent enough
                    return entry.histogram.get();
                } else { // need to refresh
                    Recorder recorder = recorders.get(host);
                    // intervalMs should be much larger than the time it takes to replace a histogram, so this future should never block
                    Histogram staleHistogram = entry.histogram.get(0, MILLISECONDS);
                    SettableFuture<Histogram> future = SettableFuture.create();
                    CachedHistogram newEntry = new CachedHistogram(future);
                    if (cachedHistograms.replace(host, entry, newEntry)) {
                        // Only get the new histogram if we successfully replaced the cache entry.
                        // This ensures that only one thread will do it.
                        Histogram newHistogram = recorder.getIntervalHistogram(staleHistogram);
                        future.set(newHistogram);
                        return newHistogram;
                    }
                    // If we couldn't replace the entry it means we raced, so loop to try again
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new DriverInternalError("Unexpected error", e.getCause());
        } catch (TimeoutException e) {
            throw new DriverInternalError("Unexpected timeout while getting histogram", e);
        }
    }

    static class CachedHistogram {
        final ListenableFuture<Histogram> histogram;
        final long timestamp;

        CachedHistogram(ListenableFuture<Histogram> histogram) {
            this.histogram = histogram;
            this.timestamp = System.currentTimeMillis();
        }

        static CachedHistogram empty() {
            return new CachedHistogram(Futures.<Histogram>immediateFuture(null));
        }
    }

    // TODO this was copy/pasted from LatencyAwarePolicy, maybe it could be refactored as a shared method
    private boolean shouldConsiderNewLatency(Statement statement, Exception exception) {
        // query was successful: always consider
        if (exception == null)
            return true;
        // filter out "fast" errors
        return !EXCLUDED_EXCEPTIONS.contains(exception.getClass());
    }

    /**
     * A set of DriverException subclasses that we should prevent from updating the host's score.
     * The intent behind it is to filter out "fast" errors: when a host replies with such errors,
     * it usually does so very quickly, because it did not involve any actual
     * coordination work. Such errors are not good indicators of the host's responsiveness,
     * and tend to make the host's score look better than it actually is.
     */
    private static final Set<Class<? extends Exception>> EXCLUDED_EXCEPTIONS = ImmutableSet.<Class<? extends Exception>>of(
            UnavailableException.class, // this is done via the snitch and is usually very fast
            OverloadedException.class,
            BootstrappingException.class,
            UnpreparedException.class,
            QueryValidationException.class // query validation also happens at early stages in the coordinator
    );

    @Override
    public void onRegister(Cluster cluster) {
        // nothing to do
    }

    @Override
    public void onUnregister(Cluster cluster) {
        // nothing to do
    }
}
