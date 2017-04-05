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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A wrapper load balancing policy that adds latency awareness to a child policy.
 * <p/>
 * When used, this policy will collect the latencies of the queries to each
 * Cassandra node and maintain a per-node latency score (an average). Based
 * on these scores, the policy will penalize (technically, it will ignore them
 * unless no other nodes are up) the nodes that are slower than the best
 * performing node by more than some configurable amount (the exclusion
 * threshold).
 * <p/>
 * The latency score for a given node is a based on a form of
 * <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">exponential moving average</a>.
 * In other words, the latency score of a node is the average of its previously
 * measured latencies, but where older measurements gets an exponentially decreasing
 * weight. The exact weight applied to a newly received latency is based on the
 * time elapsed since the previous measure (to account for the fact that
 * latencies are not necessarily reported with equal regularity, neither
 * over time nor between different nodes).
 * <p/>
 * Once a node is excluded from query plans (because its averaged latency grew
 * over the exclusion threshold), its latency score will not be updated anymore
 * (since it is not queried). To give a chance to this node to recover, the
 * policy has a configurable retry period. The policy will not penalize a host
 * for which no measurement has been collected for more than this retry period.
 * <p/>
 * Please see the {@link Builder} class and methods for more details on the
 * possible parameters of this policy.
 *
 * @since 1.0.4
 */
public class LatencyAwarePolicy implements ChainableLoadBalancingPolicy {

    private static final Logger logger = LoggerFactory.getLogger(LatencyAwarePolicy.class);

    private final LoadBalancingPolicy childPolicy;
    private final Tracker latencyTracker;
    private final ScheduledExecutorService updaterService = Executors.newSingleThreadScheduledExecutor(threadFactory("LatencyAwarePolicy updater"));

    private final double exclusionThreshold;

    private final long scale;
    private final long retryPeriod;
    private final long minMeasure;

    private LatencyAwarePolicy(LoadBalancingPolicy childPolicy,
                               double exclusionThreshold,
                               long scale,
                               long retryPeriod,
                               long updateRate,
                               int minMeasure) {
        this.childPolicy = childPolicy;
        this.retryPeriod = retryPeriod;
        this.scale = scale;
        this.latencyTracker = new Tracker();
        this.exclusionThreshold = exclusionThreshold;
        this.minMeasure = minMeasure;

        updaterService.scheduleAtFixedRate(new Updater(), updateRate, updateRate, TimeUnit.NANOSECONDS);
    }

    @Override
    public LoadBalancingPolicy getChildPolicy() {
        return childPolicy;
    }

    /**
     * Creates a new latency aware policy builder given the child policy
     * that the resulting policy should wrap.
     *
     * @param childPolicy the load balancing policy to wrap with latency
     *                    awareness.
     * @return the created builder.
     */
    public static Builder builder(LoadBalancingPolicy childPolicy) {
        return new Builder(childPolicy);
    }

    @VisibleForTesting
    class Updater implements Runnable {

        private Set<Host> excludedAtLastTick = Collections.<Host>emptySet();

        @Override
        public void run() {
            try {
                logger.trace("Updating LatencyAwarePolicy minimum");
                latencyTracker.updateMin();

                if (logger.isDebugEnabled()) {
                    /*
                     * For users to be able to know if the policy potentially needs tuning, we need to provide
                     * some feedback on on how things evolve. For that, we use the min computation to also check
                     * which host will be excluded if a query is submitted now and if any host is, we log it (but
                     * we try to avoid flooding too). This is probably interesting information anyway since it
                     * gets an idea of which host perform badly.
                     */
                    Set<Host> excludedThisTick = new HashSet<Host>();
                    double currentMin = latencyTracker.getMinAverage();
                    for (Map.Entry<Host, Snapshot.Stats> entry : getScoresSnapshot().getAllStats().entrySet()) {
                        Host host = entry.getKey();
                        Snapshot.Stats stats = entry.getValue();
                        if (stats.getMeasurementsCount() < minMeasure)
                            continue;

                        if (stats.lastUpdatedSince() > retryPeriod) {
                            if (excludedAtLastTick.contains(host))
                                logger.debug(String.format("Previously avoided host %s has not be queried since %.3fms: will be reconsidered.", host, inMS(stats.lastUpdatedSince())));
                            continue;
                        }

                        if (stats.getLatencyScore() > ((long) (exclusionThreshold * currentMin))) {
                            excludedThisTick.add(host);
                            if (!excludedAtLastTick.contains(host))
                                logger.debug(String.format("Host %s has an average latency score of %.3fms, more than %f times more than the minimum %.3fms: will be avoided temporarily.",
                                        host, inMS(stats.getLatencyScore()), exclusionThreshold, inMS(currentMin)));
                            continue;
                        }

                        if (excludedAtLastTick.contains(host)) {
                            logger.debug("Previously avoided host {} average latency has come back within accepted bounds: will be reconsidered.", host);
                        }
                    }
                    excludedAtLastTick = excludedThisTick;
                }
            } catch (RuntimeException e) {
                // An unexpected exception would suppress further execution, so catch, log, but swallow after that.
                logger.error("Error while updating LatencyAwarePolicy minimum", e);
            }
        }
    }

    private static double inMS(long nanos) {
        return ((double) nanos) / (1000 * 1000);
    }

    private static double inMS(double nanos) {
        return nanos / (1000 * 1000);
    }

    private static ThreadFactory threadFactory(String nameFormat) {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
    }

    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {
        childPolicy.init(cluster, hosts);
        cluster.register(latencyTracker);
    }

    /**
     * Returns the HostDistance for the provided host.
     *
     * @param host the host of which to return the distance of.
     * @return the HostDistance to {@code host} as returned by the wrapped policy.
     */
    @Override
    public HostDistance distance(Host host) {
        return childPolicy.distance(host);
    }

    /**
     * Returns the hosts to use for a new query.
     * <p/>
     * The returned plan will be the same as the plan generated by the
     * child policy, but with the (initial) exclusion of hosts whose recent
     * (averaged) latency is more than {@code exclusionThreshold * minLatency}
     * (where {@code minLatency} is the (averaged) latency of the fastest
     * host).
     * <p/>
     * The hosts that are initially excluded due to their latency will be returned
     * by this iterator, but only only after all non-excluded hosts of the
     * child policy have been returned.
     *
     * @param loggedKeyspace the currently logged keyspace.
     * @param statement      the statement for which to build the plan.
     * @return the new query plan.
     */
    @Override
    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
        final Iterator<Host> childIter = childPolicy.newQueryPlan(loggedKeyspace, statement);
        return new AbstractIterator<Host>() {

            private Queue<Host> skipped;

            @Override
            protected Host computeNext() {
                long min = latencyTracker.getMinAverage();
                long now = System.nanoTime();
                while (childIter.hasNext()) {
                    Host host = childIter.next();
                    TimestampedAverage latency = latencyTracker.latencyOf(host);

                    // If we haven't had enough data point yet to have a score, or the last update of the score
                    // is just too old, include the host.
                    if (min < 0 || latency == null || latency.nbMeasure < minMeasure || (now - latency.timestamp) > retryPeriod)
                        return host;

                    // If the host latency is within acceptable bound of the faster known host, return
                    // that host. Otherwise, skip it.
                    if (latency.average <= ((long) (exclusionThreshold * (double) min)))
                        return host;

                    if (skipped == null)
                        skipped = new ArrayDeque<Host>();
                    skipped.offer(host);
                }

                if (skipped != null && !skipped.isEmpty())
                    return skipped.poll();

                return endOfData();
            }

            ;
        };
    }

    /**
     * Returns a snapshot of the scores (latency averages) maintained by this
     * policy.
     *
     * @return a new (immutable) {@link Snapshot} object containing the current
     * latency scores maintained by this policy.
     */
    public Snapshot getScoresSnapshot() {
        Map<Host, TimestampedAverage> currentLatencies = latencyTracker.currentLatencies();
        ImmutableMap.Builder<Host, Snapshot.Stats> builder = ImmutableMap.builder();
        long now = System.nanoTime();
        for (Map.Entry<Host, TimestampedAverage> entry : currentLatencies.entrySet()) {
            Host host = entry.getKey();
            TimestampedAverage latency = entry.getValue();
            Snapshot.Stats stats = new Snapshot.Stats(now - latency.timestamp, latency.average, latency.nbMeasure);
            builder.put(host, stats);
        }
        return new Snapshot(builder.build());
    }

    @Override
    public void onUp(Host host) {
        childPolicy.onUp(host);
    }

    @Override
    public void onDown(Host host) {
        childPolicy.onDown(host);
        latencyTracker.resetHost(host);
    }

    @Override
    public void onAdd(Host host) {
        childPolicy.onAdd(host);
    }

    @Override
    public void onRemove(Host host) {
        childPolicy.onRemove(host);
        latencyTracker.resetHost(host);
    }

    /**
     * An immutable snapshot of the per-host scores (and stats in general)
     * maintained by {@code LatencyAwarePolicy} to base its decision upon.
     */
    public static class Snapshot {
        private final Map<Host, Stats> stats;

        private Snapshot(Map<Host, Stats> stats) {
            this.stats = stats;
        }

        /**
         * A map with the stats for all hosts tracked by the {@code
         * LatencyAwarePolicy} at the time of the snapshot.
         *
         * @return a immutable map with all the stats contained in this
         * snapshot.
         */
        public Map<Host, Stats> getAllStats() {
            return stats;
        }

        /**
         * The {@code Stats} object for a given host.
         *
         * @param host the host to return the stats of.
         * @return the {@code Stats} for {@code host} in this snapshot or
         * {@code null} if the snapshot has not information on {@code host}.
         */
        public Stats getStats(Host host) {
            return stats.get(host);
        }

        /**
         * A snapshot of the statistics on a given host kept by {@code LatencyAwarePolicy}.
         */
        public static class Stats {
            private final long lastUpdatedSince;
            private final long average;
            private final long nbMeasurements;

            private Stats(long lastUpdatedSince, long average, long nbMeasurements) {
                this.lastUpdatedSince = lastUpdatedSince;
                this.average = average;
                this.nbMeasurements = nbMeasurements;
            }

            /**
             * The number of nanoseconds since the last latency update was recorded (at the time
             * of the snapshot).
             *
             * @return The number of nanoseconds since the last latency update was recorded (at the time
             * of the snapshot).
             */
            public long lastUpdatedSince() {
                return lastUpdatedSince;
            }

            /**
             * The latency score for the host this is the stats of at the time of the snapshot.
             *
             * @return the latency score for the host this is the stats of at the time of the snapshot,
             * or {@code -1L} if not enough measurements have been taken to assign a score.
             */
            public long getLatencyScore() {
                return average;
            }

            /**
             * The number of recorded latency measurements for the host this is the stats of.
             *
             * @return the number of recorded latency measurements for the host this is the stats of.
             */
            public long getMeasurementsCount() {
                return nbMeasurements;
            }
        }
    }

    /**
     * A set of DriverException subclasses that we should prevent from updating the host's score.
     * The intent behind it is to filter out "fast" errors: when a host replies with such errors,
     * it usually does so very quickly, because it did not involve any actual
     * coordination work. Such errors are not good indicators of the host's responsiveness,
     * and tend to make the host's score look better than it actually is.
     */
    private static final Set<Class<? extends DriverException>> EXCLUDED_EXCEPTIONS = ImmutableSet.of(
            UnavailableException.class, // this is done via the snitch and is usually very fast
            OverloadedException.class,
            BootstrappingException.class,
            UnpreparedException.class,
            QueryValidationException.class // query validation also happens at early stages in the coordinator
    );

    private class Tracker implements LatencyTracker {

        private final ConcurrentMap<Host, HostLatencyTracker> latencies = new ConcurrentHashMap<Host, HostLatencyTracker>();
        private volatile long cachedMin = -1L;

        @Override
        public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
            if (shouldConsiderNewLatency(statement, exception)) {
                HostLatencyTracker hostTracker = latencies.get(host);
                if (hostTracker == null) {
                    hostTracker = new HostLatencyTracker(scale, (30L * minMeasure) / 100L);
                    HostLatencyTracker old = latencies.putIfAbsent(host, hostTracker);
                    if (old != null)
                        hostTracker = old;
                }
                hostTracker.add(newLatencyNanos);
            }
        }

        private boolean shouldConsiderNewLatency(Statement statement, Exception exception) {
            // query was successful: always consider
            if (exception == null) return true;
            // filter out "fast" errors
            if (EXCLUDED_EXCEPTIONS.contains(exception.getClass())) return false;
            return true;
        }

        public void updateMin() {
            long newMin = Long.MAX_VALUE;
            long now = System.nanoTime();
            for (HostLatencyTracker tracker : latencies.values()) {
                TimestampedAverage latency = tracker.getCurrentAverage();
                if (latency != null && latency.average >= 0 && latency.nbMeasure >= minMeasure && (now - latency.timestamp) <= retryPeriod)
                    newMin = Math.min(newMin, latency.average);
            }
            if (newMin != Long.MAX_VALUE)
                cachedMin = newMin;
        }

        public long getMinAverage() {
            return cachedMin;
        }

        public TimestampedAverage latencyOf(Host host) {
            HostLatencyTracker tracker = latencies.get(host);
            return tracker == null ? null : tracker.getCurrentAverage();
        }

        public Map<Host, TimestampedAverage> currentLatencies() {
            Map<Host, TimestampedAverage> map = new HashMap<Host, TimestampedAverage>(latencies.size());
            for (Map.Entry<Host, HostLatencyTracker> entry : latencies.entrySet())
                map.put(entry.getKey(), entry.getValue().getCurrentAverage());
            return map;
        }

        public void resetHost(Host host) {
            latencies.remove(host);
        }

        @Override
        public void onRegister(Cluster cluster) {
            // nothing to do
        }

        @Override
        public void onUnregister(Cluster cluster) {
            // nothing to do
        }
    }

    private static class TimestampedAverage {

        private final long timestamp;
        private final long average;
        private final long nbMeasure;

        TimestampedAverage(long timestamp, long average, long nbMeasure) {
            this.timestamp = timestamp;
            this.average = average;
            this.nbMeasure = nbMeasure;
        }
    }

    private static class HostLatencyTracker {

        private final long thresholdToAccount;
        private final double scale;
        private final AtomicReference<TimestampedAverage> current = new AtomicReference<TimestampedAverage>();

        HostLatencyTracker(long scale, long thresholdToAccount) {
            this.scale = (double) scale; // We keep in double since that's how we'll use it.
            this.thresholdToAccount = thresholdToAccount;
        }

        public void add(long newLatencyNanos) {
            TimestampedAverage previous, next;
            do {
                previous = current.get();
                next = computeNextAverage(previous, newLatencyNanos);
            } while (next != null && !current.compareAndSet(previous, next));
        }

        private TimestampedAverage computeNextAverage(TimestampedAverage previous, long newLatencyNanos) {

            long currentTimestamp = System.nanoTime();

            long nbMeasure = previous == null ? 1 : previous.nbMeasure + 1;
            if (nbMeasure < thresholdToAccount)
                return new TimestampedAverage(currentTimestamp, -1L, nbMeasure);

            if (previous == null || previous.average < 0)
                return new TimestampedAverage(currentTimestamp, newLatencyNanos, nbMeasure);

            // Note: it's possible for the delay to be 0, in which case newLatencyNanos will basically be
            // discarded. It's fine: nanoTime is precise enough in practice that even if it happens, it
            // will be very rare, and discarding a latency every once in a while is not the end of the world.
            // We do test for negative value, even though in theory that should not happen, because it seems
            // that historically there has been bugs here (https://blogs.oracle.com/dholmes/entry/inside_the_hotspot_vm_clocks)
            // so while this is almost surely not a problem anymore, there's no reason to break the computation
            // if this even happen.
            long delay = currentTimestamp - previous.timestamp;
            if (delay <= 0)
                return null;

            double scaledDelay = ((double) delay) / scale;
            // Note: We don't use log1p because we it's quite a bit slower and we don't care about the precision (and since we
            // refuse ridiculously big scales, scaledDelay can't be so low that scaledDelay+1 == 1.0 (due to rounding)).
            double prevWeight = Math.log(scaledDelay + 1) / scaledDelay;
            long newAverage = (long) ((1.0 - prevWeight) * newLatencyNanos + prevWeight * previous.average);

            return new TimestampedAverage(currentTimestamp, newAverage, nbMeasure);
        }

        public TimestampedAverage getCurrentAverage() {
            return current.get();
        }
    }

    /**
     * Helper builder object to create a latency aware policy.
     * <p/>
     * This helper allows to configure the different parameters used by
     * {@code LatencyAwarePolicy}. The only mandatory parameter is the child
     * policy that will be wrapped with latency awareness. The other parameters
     * can be set through the methods of this builder, but all have defaults (that
     * are documented in the javadoc of each method) if you don't.
     * <p/>
     * If you observe that the resulting policy excludes hosts too aggressively or
     * not enough so, the main parameters to check are the exclusion threshold
     * ({@link #withExclusionThreshold}) and scale ({@link #withScale}).
     *
     * @since 1.0.4
     */
    public static class Builder {

        public static final double DEFAULT_EXCLUSION_THRESHOLD = 2.0;
        public static final long DEFAULT_SCALE_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
        public static final long DEFAULT_RETRY_PERIOD_NANOS = TimeUnit.SECONDS.toNanos(10);
        public static final long DEFAULT_UPDATE_RATE_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
        public static final int DEFAULT_MIN_MEASURE = 50;

        private final LoadBalancingPolicy childPolicy;

        private double exclusionThreshold = DEFAULT_EXCLUSION_THRESHOLD;
        private long scale = DEFAULT_SCALE_NANOS;
        private long retryPeriod = DEFAULT_RETRY_PERIOD_NANOS;
        private long updateRate = DEFAULT_UPDATE_RATE_NANOS;
        private int minMeasure = DEFAULT_MIN_MEASURE;

        /**
         * Creates a new latency aware policy builder given the child policy
         * that the resulting policy wraps.
         *
         * @param childPolicy the load balancing policy to wrap with latency
         *                    awareness.
         */
        public Builder(LoadBalancingPolicy childPolicy) {
            this.childPolicy = childPolicy;
        }

        /**
         * Sets the exclusion threshold to use for the resulting latency aware policy.
         * <p/>
         * The exclusion threshold controls how much worse the average latency
         * of a node must be compared to the fastest performing node for it to be
         * penalized by the policy.
         * <p/>
         * The default exclusion threshold (if this method is not called) is <b>2</b>.
         * In other words, the resulting policy excludes nodes that are more than
         * twice slower than the fastest node.
         *
         * @param exclusionThreshold the exclusion threshold to use. Must be
         *                           greater or equal to 1.
         * @return this builder.
         * @throws IllegalArgumentException if {@code exclusionThreshold &lt; 1}.
         */
        public Builder withExclusionThreshold(double exclusionThreshold) {
            if (exclusionThreshold < 1d)
                throw new IllegalArgumentException("Invalid exclusion threshold, must be greater than 1.");
            this.exclusionThreshold = exclusionThreshold;
            return this;
        }

        /**
         * Sets the scale to use for the resulting latency aware policy.
         * <p/>
         * The {@code scale} provides control on how the weight given to older latencies
         * decreases over time. For a given host, if a new latency {@code l} is received at
         * time {@code t}, and the previously calculated average is {@code prev} calculated at
         * time {@code t'}, then the newly calculated average {@code avg} for that host is calculated
         * thusly:
         * <pre>{@code d = (t - t') / scale
         * alpha = 1 - (ln(d+1) / d)
         * avg = alpha * l + (1 - alpha * prev)}</pre>
         * Typically, with a {@code scale} of 100 milliseconds (the default), if a new
         * latency is measured and the previous measure is 10 millisecond old (so {@code d=0.1}),
         * then {@code alpha} will be around {@code 0.05}. In other words, the new latency will
         * weight 5% of the updated average. A bigger scale will get less weight to new
         * measurements (compared to previous ones), a smaller one will give them more weight.
         * <p/>
         * The default scale (if this method is not used) is of <b>100 milliseconds</b>. If unsure, try
         * this default scale first and experiment only if it doesn't provide acceptable results
         * (hosts are excluded too quickly or not fast enough and tuning the exclusion threshold
         * doesn't help).
         *
         * @param scale the scale to use.
         * @param unit  the unit of {@code scale}.
         * @return this builder.
         * @throws IllegalArgumentException if {@code scale <= 0}.
         */
        public Builder withScale(long scale, TimeUnit unit) {
            if (scale <= 0)
                throw new IllegalArgumentException("Invalid scale, must be strictly positive");
            this.scale = unit.toNanos(scale);
            return this;
        }

        /**
         * Sets the retry period for the resulting latency aware policy.
         * <p/>
         * The retry period defines how long a node may be penalized by the
         * policy before it is given a 2nd change. More precisely, a node is excluded
         * from query plans if both his calculated average latency is {@code exclusionThreshold}
         * times slower than the fastest node average latency (at the time the query plan is
         * computed) <b>and</b> his calculated average latency has been updated since
         * less than {@code retryPeriod}. Since penalized nodes will likely not see their
         * latency updated, this is basically how long the policy will exclude a node.
         *
         * @param retryPeriod the retry period to use.
         * @param unit        the unit for {@code retryPeriod}.
         * @return this builder.
         * @throws IllegalArgumentException if {@code retryPeriod &lt; 0}.
         */
        public Builder withRetryPeriod(long retryPeriod, TimeUnit unit) {
            if (retryPeriod < 0)
                throw new IllegalArgumentException("Invalid retry period, must be positive");
            this.retryPeriod = unit.toNanos(retryPeriod);
            return this;
        }

        /**
         * Sets the update rate for the resulting latency aware policy.
         * <p/>
         * The update rate defines how often the minimum average latency is
         * recomputed. While the average latency score of each node is computed
         * iteratively (updated each time a new latency is collected), the
         * minimum score needs to be recomputed from scratch every time, which
         * is slightly more costly. For this reason, the minimum is only
         * re-calculated at the given fixed rate and cached between re-calculation.
         * <p/>
         * The default update rate if <b>100 milliseconds</b>, which should be
         * appropriate for most applications. In particular, note that while we
         * want to avoid to recompute the minimum for every query, that
         * computation is not particularly intensive either and there is no
         * reason to use a very slow rate (more than second is probably
         * unnecessarily slow for instance).
         *
         * @param updateRate the update rate to use.
         * @param unit       the unit for {@code updateRate}.
         * @return this builder.
         * @throws IllegalArgumentException if {@code updateRate &lte; 0}.
         */
        public Builder withUpdateRate(long updateRate, TimeUnit unit) {
            if (updateRate <= 0)
                throw new IllegalArgumentException("Invalid update rate value, must be strictly positive");
            this.updateRate = unit.toNanos(updateRate);
            return this;
        }

        /**
         * Sets the minimum number of measurements per-host to consider for
         * the resulting latency aware policy.
         * <p/>
         * Penalizing nodes is based on an average of their recently measured
         * average latency. This average is only meaningful if a minimum of
         * measurements have been collected (moreover, a newly started
         * Cassandra node will tend to perform relatively poorly on the first
         * queries due to the JVM warmup). This is what this option controls.
         * If less that {@code minMeasure} data points have been collected for
         * a given host, the policy will never penalize that host. Also, the
         * 30% first measurement will be entirely ignored (in other words, the
         * {@code 30% * minMeasure} first measurement to a node are entirely
         * ignored, while the {@code 70%} next ones are accounted in the latency
         * computed but the node won't get convicted until we've had at least
         * {@code minMeasure} measurements).
         * <p/>
         * Note that the number of collected measurements for a given host is
         * reset if the node is restarted.
         * <p/>
         * The default for this option (if this method is not called) is <b>50</b>.
         * Note that it is probably not a good idea to put this option too low
         * if only to avoid the influence of JVM warm-up on newly restarted
         * nodes.
         *
         * @param minMeasure the minimum measurements to consider.
         * @return this builder.
         * @throws IllegalArgumentException if {@code minMeasure &lt; 0}.
         */
        public Builder withMininumMeasurements(int minMeasure) {
            if (minMeasure < 0)
                throw new IllegalArgumentException("Invalid minimum measurements value, must be positive");
            this.minMeasure = minMeasure;
            return this;
        }

        /**
         * Builds a new latency aware policy using the options set on this
         * builder.
         *
         * @return the newly created {@code LatencyAwarePolicy}.
         */
        public LatencyAwarePolicy build() {
            return new LatencyAwarePolicy(childPolicy, exclusionThreshold, scale, retryPeriod, updateRate, minMeasure);
        }
    }

    @Override
    public void close() {
        childPolicy.close();
        updaterService.shutdown();
    }
}
