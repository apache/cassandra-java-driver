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
import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Chainable load balancing policy that filters out hosts based on their error rates.
 * <p/>
 * When creating a query plan, this policy gathers a list of candidate hosts from its child policy; for each candidate
 * host, it then determines whether it should be included into or excluded from the final query plan, based on its
 * current error rate (measured over the last minute, with a 5-second granularity).
 * <p/>
 * Note that the policy should not blindly count all errors in its measurements: some type of errors (e.g. CQL syntax
 * errors) can originate from the client and occur on all hosts, therefore they should not count towards the exclusion
 * threshold or all hosts could become excluded. You can provide your own {@link ErrorFilter} to customize that logic.
 * <p/>
 * The policy follows the builder pattern to be created, the {@link Builder} class can be created with
 * {@link #builder} method.
 * <p/>
 * This policy is currently in BETA mode and its behavior might be changing throughout different driver versions.
 */
@Beta
public class ErrorAwarePolicy implements ChainableLoadBalancingPolicy {

    private static final Logger logger = LoggerFactory.getLogger(ErrorAwarePolicy.class);

    private final LoadBalancingPolicy childPolicy;

    private final long retryPeriodNanos;

    PerHostErrorTracker errorTracker;

    private ErrorAwarePolicy(Builder builder) {
        this.childPolicy = builder.childPolicy;
        this.retryPeriodNanos = builder.retryPeriodNanos;
        this.errorTracker = new PerHostErrorTracker(builder.maxErrorsPerMinute, builder.errorFilter, builder.clock);
    }

    @Override
    public LoadBalancingPolicy getChildPolicy() {
        return childPolicy;
    }

    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {
        childPolicy.init(cluster, hosts);
        cluster.register(this.errorTracker);
    }

    @Override
    public HostDistance distance(Host host) {
        return childPolicy.distance(host);
    }

    @Override
    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
        final Iterator<Host> childQueryPlan = childPolicy.newQueryPlan(loggedKeyspace, statement);

        return new AbstractIterator<Host>() {

            @Override
            protected Host computeNext() {
                while (childQueryPlan.hasNext()) {
                    Host host = childQueryPlan.next();
                    if (!errorTracker.isExcluded(host)) {
                        return host;
                    }
                }
                return endOfData();
            }
        };
    }

    @Override
    public void onAdd(Host host) {
        childPolicy.onAdd(host);
    }

    @Override
    public void onUp(Host host) {
        childPolicy.onUp(host);
    }

    @Override
    public void onDown(Host host) {
        childPolicy.onDown(host);
    }

    @Override
    public void onRemove(Host host) {
        childPolicy.onRemove(host);
    }

    /**
     * Creates a new error aware policy builder given the child policy
     * that the resulting policy should wrap.
     *
     * @param childPolicy the load balancing policy to wrap with error
     *                    awareness.
     * @return the created builder.
     */
    public static Builder builder(LoadBalancingPolicy childPolicy) {
        return new Builder(childPolicy);
    }

    @Override
    public void close() {
        childPolicy.close();
    }

    /**
     * Utility class to create a {@link ErrorAwarePolicy}.
     */
    public static class Builder {
        final LoadBalancingPolicy childPolicy;

        private int maxErrorsPerMinute = 1;
        private long retryPeriodNanos = NANOSECONDS.convert(2, MINUTES);
        private Clock clock = Clock.DEFAULT;

        private ErrorFilter errorFilter = new DefaultErrorFilter();

        /**
         * Creates a {@link Builder} instance.
         *
         * @param childPolicy the load balancing policy to wrap with error
         *                    awareness.
         */
        public Builder(LoadBalancingPolicy childPolicy) {
            this.childPolicy = childPolicy;
        }

        /**
         * Defines the maximum number of errors allowed per minute for each host.
         * <p/>
         * The policy keeps track of the number of errors on each host (filtered by
         * {@link Builder#withErrorsFilter(com.datastax.driver.core.policies.ErrorAwarePolicy.ErrorFilter)})
         * over a sliding 1-minute window. If a host had more than this number
         * of errors, it will be excluded from the query plan for the duration defined by
         * {@link #withRetryPeriod(long, TimeUnit)}.
         * <p/>
         * Default value for the threshold is 1.
         *
         * @param maxErrorsPerMinute the number.
         * @return this {@link Builder} instance, for method chaining.
         */
        public Builder withMaxErrorsPerMinute(int maxErrorsPerMinute) {
            this.maxErrorsPerMinute = maxErrorsPerMinute;
            return this;
        }

        /**
         * Defines the time during which a host is excluded by the policy once it has exceeded
         * {@link #withMaxErrorsPerMinute(int)}.
         * <p/>
         * Default value for the retry period is 2 minutes.
         *
         * @param retryPeriod         the period of exclusion for a host.
         * @param retryPeriodTimeUnit the time unit for the retry period.
         * @return this {@link Builder} instance, for method chaining.
         */
        public Builder withRetryPeriod(long retryPeriod, TimeUnit retryPeriodTimeUnit) {
            this.retryPeriodNanos = retryPeriodTimeUnit.toNanos(retryPeriod);
            return this;
        }

        /**
         * Provides a filter that will decide which errors are counted towards {@link #withMaxErrorsPerMinute(int)}.
         * <p/>
         * The default implementation will exclude from the error counting, the following exception types:
         * <ul>
         * <li>{@link QueryConsistencyException} and {@link UnavailableException}: the assumption is that these errors
         * are most often caused by other replicas being unavailable, not by something wrong on the coordinator;</li>
         * <li>{@link InvalidQueryException}, {@link AlreadyExistsException}, {@link SyntaxError}: these are likely
         * caused by a bad query in client code, that will fail on all hosts. Excluding hosts could lead to complete
         * loss of connectivity, rather the solution is to fix the query;</li>
         * <li>{@link FunctionExecutionException}: similarly, this is caused by a bad function definition and likely to
         * fail on all hosts.</li>
         * </ul>
         *
         * @param errorFilter the filter class that the policy will use.
         * @return this {@link Builder} instance, for method chaining.
         */
        public Builder withErrorsFilter(ErrorFilter errorFilter) {
            this.errorFilter = errorFilter;
            return this;
        }

        @VisibleForTesting
        Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Creates the {@link ErrorAwarePolicy} instance.
         *
         * @return the newly created {@link ErrorAwarePolicy}.
         */
        public ErrorAwarePolicy build() {
            return new ErrorAwarePolicy(this);
        }
    }

    class PerHostErrorTracker implements LatencyTracker {

        private final int maxErrorsPerMinute;
        private final ErrorFilter errorFilter;
        private final Clock clock;
        private final ConcurrentMap<Host, RollingCount> hostsCounts = new ConcurrentHashMap<Host, RollingCount>();
        private final ConcurrentMap<Host, Long> exclusionTimes = new ConcurrentHashMap<Host, Long>();

        PerHostErrorTracker(int maxErrorsPerMinute, ErrorFilter errorFilter, Clock clock) {
            this.maxErrorsPerMinute = maxErrorsPerMinute;
            this.errorFilter = errorFilter;
            this.clock = clock;
        }

        @Override
        public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
            if (exception == null) {
                return;
            }
            if (!errorFilter.shouldConsiderError(exception, host, statement)) {
                return;
            }
            RollingCount hostCount = getOrCreateCount(host);
            hostCount.increment();
        }

        boolean isExcluded(Host host) {
            Long excludedTime = exclusionTimes.get(host);
            boolean expired = excludedTime != null && clock.nanoTime() - excludedTime >= retryPeriodNanos;
            if (excludedTime == null || expired) {
                if (maybeExcludeNow(host, excludedTime)) {
                    return true;
                }
                if (expired) {
                    // Cleanup, but make sure we don't overwrite if another thread just set it
                    exclusionTimes.remove(host, excludedTime);
                }
                return false;
            } else { // host is already excluded
                return true;
            }
        }

        // Exclude if we're over the threshold
        private boolean maybeExcludeNow(Host host, Long previousTime) {
            RollingCount rollingCount = getOrCreateCount(host);
            long count = rollingCount.get();
            if (count > maxErrorsPerMinute) {
                excludeNow(host, count, previousTime);
                return true;
            } else {
                return false;
            }
        }

        // Set the exclusion time to now, handling potential races
        private void excludeNow(Host host, long count, Long previousTime) {
            long now = clock.nanoTime();
            boolean didNotRace = (previousTime == null)
                    ? exclusionTimes.putIfAbsent(host, now) == null
                    : exclusionTimes.replace(host, previousTime, now);

            if (didNotRace && logger.isDebugEnabled()) {
                logger.debug(String.format("Host %s encountered %d errors in the last minute, which is more " +
                                "than the maximum allowed (%d). It will be excluded from query plans for the " +
                                "next %d nanoseconds.",
                        host, count, maxErrorsPerMinute, retryPeriodNanos));
            }
        }

        private RollingCount getOrCreateCount(Host host) {
            RollingCount hostCount = hostsCounts.get(host);
            if (hostCount == null) {
                RollingCount tmp = new RollingCount(clock);
                hostCount = hostsCounts.putIfAbsent(host, tmp);
                if (hostCount == null)
                    hostCount = tmp;
            }
            return hostCount;
        }

        @Override
        public void onRegister(Cluster cluster) {
            // nothing to do.
        }

        @Override
        public void onUnregister(Cluster cluster) {
            // nothing to do.
        }
    }

    static class DefaultErrorFilter implements ErrorFilter {
        private static final List<Class<? extends Exception>> IGNORED_EXCEPTIONS =
                ImmutableList.<Class<? extends Exception>>builder()
                        .add(FunctionExecutionException.class)
                        .add(QueryConsistencyException.class)
                        .add(UnavailableException.class)
                        .add(AlreadyExistsException.class)
                        .add(InvalidQueryException.class)
                        .add(SyntaxError.class)
                        .build();

        @Override
        public boolean shouldConsiderError(Exception e, Host host, Statement statement) {
            for (Class<? extends Exception> ignoredException : IGNORED_EXCEPTIONS) {
                if (ignoredException.isInstance(e))
                    return false;
            }
            return true;
        }
    }

    /**
     * A filter for the errors considered by {@link ErrorAwarePolicy}.
     * <p/>
     * Only errors that indicate something wrong with a host should lead to its exclusion from query plans.
     */
    public interface ErrorFilter {
        /**
         * Whether an error should be counted in the host's error rate.
         *
         * @param e         the exception.
         * @param host      the host.
         * @param statement the statement that caused the exception.
         * @return {@code true} if the exception should be counted.
         */
        boolean shouldConsiderError(Exception e, Host host, Statement statement);
    }
}
