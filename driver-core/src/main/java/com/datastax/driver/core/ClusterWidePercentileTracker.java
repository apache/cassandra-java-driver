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

/**
 * A {@code PercentileTracker} that aggregates all measurements into a single histogram.
 * <p/>
 * This gives you global latency percentiles for the whole cluster, meaning that latencies of slower hosts will tend to
 * appear in higher percentiles.
 */
public class ClusterWidePercentileTracker extends PercentileTracker {
    private volatile Cluster cluster;

    private ClusterWidePercentileTracker(long highestTrackableLatencyMillis,
                                         int numberOfSignificantValueDigits,
                                         int minRecordedValues,
                                         long intervalMs) {
        super(highestTrackableLatencyMillis, numberOfSignificantValueDigits, minRecordedValues, intervalMs);
    }

    @Override
    public void onRegister(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    protected Cluster computeKey(Host host, Statement statement, Exception exception) {
        return cluster;
    }

    /**
     * Returns a builder to create a new instance.
     *
     * @param highestTrackableLatencyMillis the highest expected latency. If a higher value is reported, it will be
     *                                      ignored and a warning will be logged. A good rule of thumb is to set it
     *                                      slightly higher than {@link SocketOptions#getReadTimeoutMillis()}.
     * @return the builder.
     */
    public static Builder builder(long highestTrackableLatencyMillis) {
        return new Builder(highestTrackableLatencyMillis);
    }

    /**
     * Helper class to build {@code PerHostPercentileTracker} instances with a fluent interface.
     */
    public static class Builder extends PercentileTracker.Builder<Builder, ClusterWidePercentileTracker> {

        Builder(long highestTrackableLatencyMillis) {
            super(highestTrackableLatencyMillis);
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public ClusterWidePercentileTracker build() {
            return new ClusterWidePercentileTracker(highestTrackableLatencyMillis, numberOfSignificantValueDigits,
                    minRecordedValues, intervalMs);
        }
    }
}
