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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.LatencyTracker;
import com.datastax.driver.core.PercentileTracker;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A policy that triggers speculative executions when the request time to the current host is above a given maxDelay
 * or when request time is in given range (min, max) and above given percentile.
 */
public class MixSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {
    private final int maxSpeculativeExecutions;

    private final long constantMinDelayMillis;
    private final long constantMaxDelayMillis;

    private final PercentileTracker percentileTracker;
    private final double percentile;

    /**
     * Builds a new instance.
     *
     * @param constantMinDelayMillis   the min delay between each speculative execution. Must be >= 0.
     * @param constantMaxDelayMillis   the max delay between each speculative execution. Must be >= 0. A zero delay means
     *                                 it should immediately send `maxSpeculativeExecutions` requests along with the
     *                                 original request. If delay is between min and max then percentile takes care
     *                                 about starting next speculative execution.
     * @param maxSpeculativeExecutions the number of speculative executions. Must be strictly positive.
     * @param percentileTracker        the component that will record latencies. It will get
     *                                 {@link Cluster#register(LatencyTracker) registered} with the cluster when this
     *                                 policy initializes.
     * @param percentile               the percentile that a request's latency must fall into to be considered slow (ex:
     *                                 {@code 99.0}).
     * @param maxSpeculativeExecutions the maximum number of speculative executions that will be triggered for a given
     *                                 request (this does not include the initial, normal request). Must be strictly
     *                                 positive.
     * @throws IllegalArgumentException if one of the arguments does not respect the preconditions above.
     */
    public MixSpeculativeExecutionPolicy(final long constantMinDelayMillis, final long constantMaxDelayMillis,
                                         final PercentileTracker percentileTracker, final double percentile,
                                         final int maxSpeculativeExecutions) {
        Preconditions.checkArgument(constantMinDelayMillis > 0,
                "min delay must be strictly positive (was %d)", constantMinDelayMillis);
        Preconditions.checkArgument(constantMaxDelayMillis > 0,
                "max delay must be strictly positive (was %d)", constantMaxDelayMillis);
        Preconditions.checkArgument(maxSpeculativeExecutions > 0,
                "number of speculative executions must be strictly positive (was %d)", maxSpeculativeExecutions);
        this.constantMinDelayMillis = constantMinDelayMillis;
        this.constantMaxDelayMillis = constantMaxDelayMillis;
        this.maxSpeculativeExecutions = maxSpeculativeExecutions;

        checkArgument(percentile >= 0.0 && percentile < 100, "percentile must be between 0.0 and 100 (was %f)");

        this.percentileTracker = percentileTracker;
        this.percentile = percentile;
    }

    @Override
    public SpeculativeExecutionPlan newPlan(String loggedKeyspace, Statement statement) {
        return new SpeculativeExecutionPlan() {
            private final AtomicInteger remaining = new AtomicInteger(maxSpeculativeExecutions);

            @Override
            public long nextExecution(Host lastQueried) {
                if (remaining.getAndDecrement() > 0) {
                    long latencyAtPercentile = percentileTracker.getLatencyAtPercentile(lastQueried, null, null, percentile);
                    return Math.max(Math.min(latencyAtPercentile, constantMaxDelayMillis), constantMinDelayMillis);
                } else
                    return -1;
            }
        };
    }

    @Override
    public void init(Cluster cluster) {
        cluster.register(percentileTracker);
    }

    @Override
    public void close() {
        // nothing
    }
}
