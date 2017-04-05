/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.*;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.*;

/**
 * A {@link LatencyTracker} that records query latencies over a sliding time interval, and exposes an API to retrieve
 * the latency at a given percentile.
 * <p/>
 * Percentiles may be computed separately for different categories of requests; this is implementation-dependent and
 * determined by {@link #computeKey(Host, Statement, Exception)}.
 * <p/>
 * This class is used by percentile-aware components such as
 * {@link QueryLogger.Builder#withDynamicThreshold(PercentileTracker, double)}  QueryLogger} and
 * {@link com.datastax.driver.core.policies.PercentileSpeculativeExecutionPolicy}.
 * <p/>
 * It uses <a href="http://hdrhistogram.github.io/HdrHistogram/">HdrHistogram</a> to record latencies:
 * for each category, there is a "live" histogram where current latencies are recorded, and a "cached", read-only
 * histogram that is used when clients call {@link #getLatencyAtPercentile(Host, Statement, Exception, double)}. Each
 * time the cached histogram becomes older than the interval, the two histograms are switched. Statistics will not be
 * available during the first interval at cluster startup, since we don't have a cached histogram yet.
 */
public abstract class PercentileTracker implements LatencyTracker {
    private static final Logger logger = LoggerFactory.getLogger(PercentileTracker.class);

    private final long highestTrackableLatencyMillis;
    private final int numberOfSignificantValueDigits;
    private final int minRecordedValues;
    private final long intervalMs;

    // The "live" recorders: this is where we store the latencies received from the cluster
    private final ConcurrentMap<Object, Recorder> recorders;
    // The cached histograms, corresponding to the previous interval. This is where we get the percentiles from when the
    // user requests them. Each histogram is valid for a given duration, when it gets stale we request a new one from
    // the corresponding recorder.
    private final ConcurrentMap<Object, CachedHistogram> cachedHistograms;

    /**
     * Builds a new instance.
     *
     * @see Builder
     */
    protected PercentileTracker(long highestTrackableLatencyMillis,
                                int numberOfSignificantValueDigits,
                                int minRecordedValues,
                                long intervalMs) {
        this.highestTrackableLatencyMillis = highestTrackableLatencyMillis;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
        this.minRecordedValues = minRecordedValues;
        this.intervalMs = intervalMs;
        this.recorders = new ConcurrentHashMap<Object, Recorder>();
        this.cachedHistograms = new ConcurrentHashMap<Object, CachedHistogram>();
    }

    /**
     * Computes a key used to categorize measurements. Measurements with the same key will be recorded in the same
     * histogram.
     * <p/>
     * It's recommended to keep the number of distinct keys low, in order to limit the memory footprint of the
     * histograms.
     *
     * @param host      the host that was queried.
     * @param statement the statement that was executed.
     * @param exception if the query failed, the corresponding exception.
     * @return the key.
     */
    protected abstract Object computeKey(Host host, Statement statement, Exception exception);

    @Override
    public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
        if (!include(host, statement, exception))
            return;

        long latencyMs = NANOSECONDS.toMillis(newLatencyNanos);
        try {
            Recorder recorder = getRecorder(host, statement, exception);
            if (recorder != null)
                recorder.recordValue(latencyMs);
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.warn("Got request with latency of {} ms, which exceeds the configured maximum trackable value {}",
                    latencyMs, highestTrackableLatencyMillis);
        }
    }

    /**
     * Returns the request latency at a given percentile.
     *
     * @param host       the host (if this is relevant in the way percentiles are categorized).
     * @param statement  the statement (if this is relevant in the way percentiles are categorized).
     * @param exception  the exception (if this is relevant in the way percentiles are categorized).
     * @param percentile the percentile (for example, {@code 99.0} for the 99th percentile).
     * @return the latency (in milliseconds) at the given percentile, or a negative value if it's not available yet.
     * @see #computeKey(Host, Statement, Exception)
     */
    public long getLatencyAtPercentile(Host host, Statement statement, Exception exception, double percentile) {
        checkArgument(percentile >= 0.0 && percentile < 100,
                "percentile must be between 0.0 and 100 (was %s)", percentile);
        Histogram histogram = getLastIntervalHistogram(host, statement, exception);
        if (histogram == null || histogram.getTotalCount() < minRecordedValues)
            return -1;

        return histogram.getValueAtPercentile(percentile);
    }

    private Recorder getRecorder(Host host, Statement statement, Exception exception) {
        Object key = computeKey(host, statement, exception);
        if (key == null)
            return null;

        Recorder recorder = recorders.get(key);
        if (recorder == null) {
            recorder = new Recorder(highestTrackableLatencyMillis, numberOfSignificantValueDigits);
            Recorder old = recorders.putIfAbsent(key, recorder);
            if (old != null) {
                // We got beaten at creating the recorder, use the actual instance and discard ours
                recorder = old;
            } else {
                // Also set an empty cache entry to remember the time we started recording:
                cachedHistograms.putIfAbsent(key, CachedHistogram.empty());
            }
        }
        return recorder;
    }

    /**
     * @return null if no histogram is available yet (no entries recorded, or not for long enough)
     */
    private Histogram getLastIntervalHistogram(Host host, Statement statement, Exception exception) {
        Object key = computeKey(host, statement, exception);
        if (key == null)
            return null;

        try {
            while (true) {
                CachedHistogram entry = cachedHistograms.get(key);
                if (entry == null)
                    return null;

                long age = System.currentTimeMillis() - entry.timestamp;
                if (age < intervalMs) { // current histogram is recent enough
                    return entry.histogram.get();
                } else { // need to refresh
                    Recorder recorder = recorders.get(key);
                    // intervalMs should be much larger than the time it takes to replace a histogram, so this future should never block
                    Histogram staleHistogram = entry.histogram.get(0, MILLISECONDS);
                    SettableFuture<Histogram> future = SettableFuture.create();
                    CachedHistogram newEntry = new CachedHistogram(future);
                    if (cachedHistograms.replace(key, entry, newEntry)) {
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

    /**
     * A histogram and the timestamp at which it was retrieved.
     * The data is only relevant for (timestamp + intervalMs); after that, the histogram is stale and we want to
     * retrieve a new one.
     */
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

    @Override
    public void onRegister(Cluster cluster) {
        // nothing by default
    }

    @Override
    public void onUnregister(Cluster cluster) {
        // nothing by default
    }

    /**
     * Determines whether a particular measurement should be included.
     * <p/>
     * This is used to ignore measurements that could skew the statistics; for example, we typically want to ignore
     * invalid query errors because they have a very low latency and would make a given cluster/host appear faster than
     * it really is.
     *
     * @param host      the host that was queried.
     * @param statement the statement that was executed.
     * @param exception if the query failed, the corresponding exception.
     * @return whether the measurement should be included.
     */
    protected boolean include(Host host, Statement statement, Exception exception) {
        // query was successful: always consider
        if (exception == null)
            return true;
        // filter out "fast" errors
        // TODO this was copy/pasted from LatencyAwarePolicy, maybe it could be refactored as a shared method
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

    /**
     * Base class for {@code PercentileTracker} implementation builders.
     *
     * @param <B> the type of the concrete builder implementation.
     * @param <T> the type of the object to build.
     */
    public static abstract class Builder<B, T> {
        protected final long highestTrackableLatencyMillis;
        protected int numberOfSignificantValueDigits = 3;
        protected int minRecordedValues = 1000;
        protected long intervalMs = MINUTES.toMillis(5);

        Builder(long highestTrackableLatencyMillis) {
            this.highestTrackableLatencyMillis = highestTrackableLatencyMillis;
        }

        protected abstract B self();

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
        public B withNumberOfSignificantValueDigits(int numberOfSignificantValueDigits) {
            this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
            return self();
        }

        /**
         * Sets the minimum number of values that must be recorded for a host before we consider
         * the sample size significant.
         * <p/>
         * If this count is not reached during a given interval,
         * {@link #getLatencyAtPercentile(Host, Statement, Exception, double)} will return a negative value, indicating
         * that statistics are not available. In particular, this is true during the first interval.
         * <p/>
         * If not set explicitly, this value default to 1000.
         *
         * @param minRecordedValues the new value.
         * @return this builder.
         */
        public B withMinRecordedValues(int minRecordedValues) {
            this.minRecordedValues = minRecordedValues;
            return self();
        }

        /**
         * Sets the time interval over which samples are recorded.
         * <p/>
         * For each host, there is a "live" histogram where current latencies are recorded, and a "cached", read-only
         * histogram that is used when clients call {@link #getLatencyAtPercentile(Host, Statement, Exception, double)}.
         * Each time the cached histogram becomes older than the interval, the two histograms are switched. Note that
         * statistics will not be available during the first interval at cluster startup, since we don't have a cached
         * histogram yet.
         * <p/>
         * If not set explicitly, this value defaults to 5 minutes.
         *
         * @param interval the new interval.
         * @param unit     the unit that the interval is expressed in.
         * @return this builder.
         */
        public B withInterval(long interval, TimeUnit unit) {
            this.intervalMs = MILLISECONDS.convert(interval, unit);
            return self();
        }

        /**
         * Builds the {@code PercentileTracker} instance configured with this builder.
         *
         * @return the instance.
         */
        public abstract T build();
    }

}
