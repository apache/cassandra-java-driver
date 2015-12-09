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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PerHostPercentileTracker;
import com.datastax.driver.core.Statement;
import com.google.common.annotations.Beta;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A policy that triggers speculative executions when the request to the current host is above a given percentile.
 * <p/>
 * This class uses a {@link PerHostPercentileTracker} that must be registered with the cluster instance:
 * <pre>
 * PerHostPercentileTracker tracker = PerHostPercentileTracker
 *     .builderWithHighestTrackableLatencyMillis(15000)
 *     .build();
 * PercentileSpeculativeExecutionPolicy policy = new PercentileSpeculativeExecutionPolicy(tracker, 99.0, 2);
 * cluster = Cluster.builder()
 *     .addContactPoint("127.0.0.1")
 *     .withSpeculativeExecutionPolicy(policy)
 *     .build();
 * cluster.register(tracker);
 * </pre>
 * You <b>must</b> register the tracker with the cluster yourself (as shown on the last line above), this class will not
 * do it itself.
 * <p/>
 * <b>This class is currently provided as a beta preview: it hasn't been extensively tested yet, and the API is still subject
 * to change.</b>
 */
@Beta
public class PercentileSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {
    private final PerHostPercentileTracker percentileTracker;
    private final double percentile;
    private final int maxSpeculativeExecutions;

    /**
     * Builds a new instance.
     *
     * @param percentileTracker        the component that will record latencies. Note that this policy doesn't register it with the {@code Cluster},
     *                                 you <b>must</b> do it yourself (see the code example in this class's Javadoc).
     * @param percentile               the percentile that a request's latency must fall into to be considered slow (ex: {@code 99.0}).
     * @param maxSpeculativeExecutions the maximum number of speculative executions that will be triggered for a given request (this does not
     *                                 include the initial, normal request). Must be strictly positive.
     */
    public PercentileSpeculativeExecutionPolicy(PerHostPercentileTracker percentileTracker, double percentile, int maxSpeculativeExecutions) {
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
                    return percentileTracker.getLatencyAtPercentile(lastQueried, percentile);
                else
                    return -1;
            }
        };
    }

    @Override
    public void init(Cluster cluster) {
        // nothing
    }

    @Override
    public void close() {
        // nothing
    }
}
