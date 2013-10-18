/*
 *      Copyright (C) 2012 DataStax Inc.
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
package com.datastax.driver.core.policies;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;

/**
 * A wrapper load balancing policy that adds latency awareness to a child policy.
 * <p>
 * When used, this policy will collect the latencies of the queries to each
 * Cassandra and maintain, for each node, a latency score (an average). Based
 * on those scores, the policy will penalize (technically, it will ignore them
 * unless no other nodes are up) the nodes that are slower than the best
 * performing node by more than some configurable amount (the exclusion
 * threshold).
 * <p>
 * The latency score for a given node is a based on a form of
 * <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">exponential moving average</a>.
 * In other words, the latency score of a node is the average of its previously
 * measured latencies, but where older measurements gets an exponentially decreasing
 * weight. The exact weight applied to a newly received latency is based on the
 * delay elapsed since the previous measure (to account for the fact that
 * latencies are not necessarily reported with equal regularity, neither
 * over time nor between different nodes).
 * <p>
 * Once a node is excluded from query plans (because its averaged latency grow
 * over the exclusion threshold), its latency score will not evolve anymore
 * (since it is not queried). To give a chance to those node to recover, the
 * policy has a configurable retry period. The policy will not penalize a host
 * for which no measurement has been collected for more than this retry period.
 * <p>
 * Please see the {@link Builder} class and methods for more details on the
 * possible paramters of this policy.
 *
 * @since 1.0.4
 */
public class LatencyAwarePolicy implements LoadBalancingPolicy {

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

    /**
     * Creates a new latency aware policy builder given the child policy
     * that the resulting policy should wrap.
     *
     * @param childPolicy the load balancing policy to wrap with latency
     * awareness.
     * @return the created builder.
     */
    public static Builder builder(LoadBalancingPolicy childPolicy) {
        return new Builder(childPolicy);
    }

    private class Updater implements Runnable {

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
                    double currentMin = latencyTracker.getMinAverage();
                    Map<Host, TimestampedAverage> currentLatencies = latencyTracker.currentLatencies();
                    Set<Host> excludedThisTick = new HashSet<Host>();
                    long now = System.nanoTime();
                    for (Map.Entry<Host, TimestampedAverage> entry : currentLatencies.entrySet()) {
                        Host host = entry.getKey();
                        TimestampedAverage latency = entry.getValue();
                        if (latency.nbMeasure < minMeasure)
                            continue;

                        if ((now - latency.timestamp) > retryPeriod) {
                            if (excludedAtLastTick.contains(host))
                                logger.debug(String.format("Previously avoided host %s has not be queried since %.3fms: will be reconsidered.", host, inMS(now - latency.timestamp)));
                            continue;
                        }

                        if (latency.average > ((long)(exclusionThreshold * currentMin))) {
                            excludedThisTick.add(host);
                            if (!excludedAtLastTick.contains(host))
                                logger.debug(String.format("Host %s has an average latency score of %.3fms, more than %f times more than the minimum %.3fms: will be avoided temporarily.",
                                                          host, inMS(latency.average), exclusionThreshold, inMS(currentMin)));
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
        return ((double)nanos) / (1000 * 1000);
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
     * Return the HostDistance for the provided host.
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
     * <p>
     * The returned plan will be the same as the plan generated by the
     * child policy, but with the (initial) exclusion of hosts whose recent
     * (averaged) latency is more than {@code exclusionThreshold * minLatency}
     * (where {@code minLatency} is the (averaged) latency of the fastest
     * host).
     * <p>
     * The hosts that are initally excluded due to their latency will be returned
     * by the returned iterator, but only only after all non-excluded host of the
     * child policy have been returned.
     *
     * @param loggedKeyspace the currently logged keyspace.
     * @param statement the statement for which to build the plan.
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
                    if (latency.average <= ((long)(exclusionThreshold * (double)min)))
                        return host;

                    if (skipped == null)
                        skipped = new ArrayDeque<Host>();
                    skipped.offer(host);
                }

                if (skipped != null && !skipped.isEmpty())
                    return skipped.poll();

                return endOfData();
            };
        };
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

    private class Tracker implements LatencyTracker {

        private final ConcurrentMap<Host, HostLatencyTracker> latencies = new ConcurrentHashMap<Host, HostLatencyTracker>();
        private volatile long cachedMin = -1L;

        public void update(Host host, long newLatencyNanos) {
            HostLatencyTracker hostTracker = latencies.get(host);
            if (hostTracker == null) {
                hostTracker = new HostLatencyTracker(scale);
                HostLatencyTracker old = latencies.putIfAbsent(host, hostTracker);
                if (old != null)
                    hostTracker = old;
            }
            hostTracker.add(newLatencyNanos);
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
    }

    private static class TimestampedAverage {

        private final long timestamp;
        private final long average;
        private final int nbMeasure;

        TimestampedAverage(long timestamp, long average, int nbMeasure) {
            this.timestamp = timestamp;
            this.average = average;
            this.nbMeasure = nbMeasure;
        }
    }

    private static class HostLatencyTracker {

        private final double scale;
        private final AtomicReference<TimestampedAverage> current = new AtomicReference<TimestampedAverage>();

        HostLatencyTracker(long scale) {
            this.scale = (double)scale; // We keep in double since that's how we'll use it.
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

            if (previous == null)
                return new TimestampedAverage(currentTimestamp, newLatencyNanos, 1);

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

            double scaledDelay = ((double)delay)/scale;
            // Note: We don't use log1p because we it's quite a bit slower and we don't care about the precision (and since we
            // refuse ridiculously big scales, scaledDelay can't be so low that scaledDelay+1 == 1.0 (due to rounding)).
            double prevWeight = Math.log(scaledDelay+1) / scaledDelay;
            long newAverage = (long)((1.0 - prevWeight) * newLatencyNanos + prevWeight * previous.average);

            return new TimestampedAverage(currentTimestamp, newAverage, previous.nbMeasure+1);
        }

        public TimestampedAverage getCurrentAverage() {
            return current.get();
        }
    }

    /**
     * Helper builder object to create a latency aware policy.
     * <p>
     * This helper allows to configure the different parameters used by
     * {@code LatencyAwarePolicy}. The only mandatory option is the child
     * policy that will be wrapped with latency awareness. The other parameters
     * can be set through the method of this builder but all have defaults (that
     * are documented in the javadoc of each method) if you don't.
     * <p>
     * If you observe that the resulting policy exclude host too agressively or
     * not enough so, the main parameter to check are the exclusion threashold
     * ({@link #withExclusionThreshold}) and scale ({@link #withScale}).
     *
     * @since 1.0.4
     */
    public static class Builder {

        private static final double DEFAULT_EXCLUSION_THRESHOLD = 2.0;
        private static final long DEFAULT_SCALE = TimeUnit.MILLISECONDS.toNanos(100);
        private static final long DEFAULT_RETRY_PERIOD = TimeUnit.SECONDS.toNanos(10);
        private static final long DEFAULT_UPDATE_RATE = TimeUnit.MILLISECONDS.toNanos(100);
        private static final int DEFAULT_MIN_MEASURE = 50;

        private final LoadBalancingPolicy childPolicy;

        private double exclusionThreshold = DEFAULT_EXCLUSION_THRESHOLD;
        private long scale = DEFAULT_SCALE;
        private long retryPeriod = DEFAULT_RETRY_PERIOD;
        private long updateRate = DEFAULT_UPDATE_RATE;
        private int minMeasure = DEFAULT_MIN_MEASURE;

        /**
         * Creates a new latency aware policy builder given the child policy
         * that the resulting policy should wrap.
         *
         * @param childPolicy the load balancing policy to wrap with latency
         * awareness.
         */
        public Builder(LoadBalancingPolicy childPolicy) {
            this.childPolicy = childPolicy;
        }

        /**
         * Sets the exclusion threshold to use for the resulting latency aware policy.
         * <p>
         * The exclusion threshold controls how much worst the average latency
         * of a node must be compared to the faster performing one for it to be
         * penalized by the policy.
         * <p>
         * The default exclusion threshold (if this method is not called) is 2.
         * In other words, the resulting policy excludes nodes that are more than
         * twice slower than the fastest node.
         *
         * @param exclusionThreshold the exclusion threshold to use. Must be
         * greater or equal to 1.
         * @return this builder.
         *
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
         * <p>
         * The {@code scale} provides control on how the weight given to older latencies
         * decrease over time. For a given host, if a new latency \(l\) is received at
         * time \(t\), and the previously calculated average is \(prev\) calculated at
         * time \(t'\), then the newly calculated average \(avg\) for that host is calculated
         * thusly:
         * \[
         *   d = \frac{t - t'}{scale} \\
         *   \alpha = 1 - \left(\frac{\ln(d+1)}{d}\right) \\
         *   avg = \alpha * l + (1-\alpha) * prev
         * \]
         * Typically, with a {@code scale} of 100 milliseconds (the default), if a new
         * latency is measured and the previous measure is 10 millisecond old (so \(d=0.1\)),
         * then \(\alpha\) will be around \(0.05\). In other words, the new latency will
         * weight 5% of the updated average. A bigger scale will get less weight to new
         * measurements (compared to previous ones), a smaller one will give them more weight.
         * <p>
         * The default scale (if this method is not used) is of 10 milliseconds. If unsure, try
         * this default scale first and experiment only if it doesn't provide acceptable results
         * (hosts are excluded too quickly or not fast enough and tuning the exclusion threshold
         * doesn't help).
         *
         * @param scale the scale to use.
         * @param unit the unit of {@code scale}.
         * @return this builder.
         *
         * @throws IllegalArgumentException if {@code scale &lte; 0}.
         */
        public Builder withScale(long scale, TimeUnit unit) {
            if (scale <= 0)
                throw new IllegalArgumentException("Invalid scale, must be strictly positive");
            return this;
        }

        /**
         * Sets the retry period for the resulting latency aware policy.
         * <p>
         * The retry period defines how long a node may be penalized by the
         * policy before it is given a 2nd change. More precisely, a node is excluded
         * from query plans if both his calculated average latency is {@code exclusionThreshold}
         * times slower than the fastest average latency (at the time the query plan is
         * computed) <b>and</b> his calculated average latency has been updated since
         * less than {@code retryPeriod}. Since penalized nodes will likely not see their
         * latency updated, this basically how long the policy will exclude a node.
         *
         * @param retryPeriod the retry period to use.
         * @param unit the unit for {@code retryPeriod}.
         * @return this builder.
         *
         * @throws IllegalArgumentException if {@code retryPeriod &lt; 0}.
         */
        public Builder withRetryPeriod(long retryPeriod, TimeUnit unit) {
            if (retryPeriod < 0)
                throw new IllegalArgumentException("Invalid retry period, must be positive");
            this.retryPeriod = unit.toNanos(retryPeriod);
            return this;
        }

        /**
         * Set the update rate for the resulting latency aware policy.
         *
         * The update rate defines how often the minimum average latency is
         * recomputed. While the average latency score of each node is computed
         * iteratively (updated each time a new latency is collected), the
         * minimum score needs to be recomputed from scratch every time, which
         * is slightly more costly. For that reason, that minimum is only
         * re-calculated at fixed rate and cached between re-calculation.
         * <p>
         * The default update rate if 100 milliseconds, which should be
         * appropriate for most applications. In particular, note that while we
         * want to avoid to recompute the minimum for every query, that
         * computation is not particularly intensive either and there is no
         * reason to use a very slow rate (more than second is probably
         * unecessarily slow for instance).
         *
         * @param updateRate the update rate to use.
         * @param unit the unit for {@code updateRate}.
         * @return this builder.
         *
         * @throws IllegalArgumentException if {@code updateRate &lte; 0}.
         */
        public Builder withUpdateRate(long updateRate, TimeUnit unit) {
            if (updateRate <= 0)
                throw new IllegalArgumentException("Invalid update rate value, must be strictly positive");
            this.updateRate = unit.toNanos(updateRate);
            return this;
        }

        /**
         * Sets the minimimum number of measurements per-host to consider for
         * the resulting latency aware policy.
         * <p>
         * Penalizing nodes is based on an average of their recently measured
         * average latency. That average is only meaningful if a minimum of
         * measurements have been collected (moreover, a newly started
         * Cassandra node will tend to perform relatively poorly on the first
         * queries due to the JVM warmup). This is what this option controls.
         * If less that {@code minMeasure} data points have been collected for
         * a given host, the policy will never penalize that host. Note that
         * the number of collected measurements for a given host is resetted
         * <p>
         * The default for this option (if this method is not called) is 50.
         * Note that it is probably not a good idea to put this option too low
         * if only to avoid the influence of JVM warmup on newly restarted
         * nodes.
         *
         * @param minMeasure the minimum measurements to consider.
         * @return this builder.
         *
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
}
