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

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A policy that triggers speculative executions when the request to the current host is above a given percentile.
 */
public class PercentileSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {
    private final PercentileTracker percentileTracker;
    private final double percentile;
    private final int maxSpeculativeExecutions;

    /**
     * Builds a new instance.
     *
     * @param percentileTracker        the component that will record latencies. It will get
     *                                 {@link Cluster#register(LatencyTracker) registered} with the cluster when this
     *                                 policy initializes.
     * @param percentile               the percentile that a request's latency must fall into to be considered slow (ex:
     *                                 {@code 99.0}).
     * @param maxSpeculativeExecutions the maximum number of speculative executions that will be triggered for a given
     *                                 request (this does not include the initial, normal request). Must be strictly
     *                                 positive.
     */
    public PercentileSpeculativeExecutionPolicy(PercentileTracker percentileTracker,
                                                double percentile, int maxSpeculativeExecutions) {
        checkArgument(maxSpeculativeExecutions > 0,
                "number of speculative executions must be strictly positive (was %d)", maxSpeculativeExecutions);
        checkArgument(percentile >= 0.0 && percentile < 100,
                "percentile must be between 0.0 and 100 (was %f)");

        this.percentileTracker = percentileTracker;
        this.percentile = percentile;
        this.maxSpeculativeExecutions = maxSpeculativeExecutions;
    }

    @Override
    public SpeculativeExecutionPlan newPlan(String loggedKeyspace, Statement statement) {
        return new SpeculativeExecutionPlan() {
            private final AtomicInteger remaining = new AtomicInteger(maxSpeculativeExecutions);

            @Override
            public long nextExecution(Host lastQueried) {
                if (remaining.getAndDecrement() > 0)
                    return percentileTracker.getLatencyAtPercentile(lastQueried, null, null, percentile);
                else
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
