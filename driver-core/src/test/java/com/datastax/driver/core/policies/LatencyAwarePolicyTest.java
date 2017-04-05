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
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.UnavailableException;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.scassandra.http.client.PrimingRequest.queryBuilder;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.Result.read_request_timeout;
import static org.scassandra.http.client.Result.unavailable;

public class LatencyAwarePolicyTest extends ScassandraTestBase {

    /**
     * A special latency tracker used to signal to the main thread that all trackers have finished their jobs.
     */
    private class LatencyTrackerBarrier implements LatencyTracker {

        private final CountDownLatch latch;

        private LatencyTrackerBarrier(int numberOfQueries) {
            latch = new CountDownLatch(numberOfQueries);
        }

        @Override
        public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
            latch.countDown();
        }

        public void await() throws InterruptedException {
            latch.await(10, SECONDS);
        }

        @Override
        public void onRegister(Cluster cluster) {
        }

        @Override
        public void onUnregister(Cluster cluster) {
        }
    }

    @Test(groups = "short")
    public void should_consider_latency_when_query_successful() throws Exception {
        // given
        String query = "SELECT foo FROM bar";
        primingClient.prime(
                queryBuilder()
                        .withQuery(query)
                        .build()
        );
        LatencyAwarePolicy latencyAwarePolicy = LatencyAwarePolicy.builder(new RoundRobinPolicy())
                .withMininumMeasurements(1)
                .build();
        Cluster.Builder builder = super.createClusterBuilder();
        builder.withLoadBalancingPolicy(latencyAwarePolicy);
        Cluster cluster = builder.build();
        try {
            cluster.init(); // force initialization of latency aware policy
            LatencyTrackerBarrier barrier = new LatencyTrackerBarrier(1);
            cluster.register(barrier); // add barrier to synchronize latency tracker threads with the current thread
            Session session = cluster.connect();
            // when
            session.execute(query);
            // then
            // wait until trackers have been notified
            barrier.await();
            // make sure the updater is called at least once
            latencyAwarePolicy.new Updater().run();
            LatencyAwarePolicy.Snapshot snapshot = latencyAwarePolicy.getScoresSnapshot();
            assertThat(snapshot.getAllStats()).hasSize(1);
            LatencyAwarePolicy.Snapshot.Stats stats = snapshot.getStats(retrieveSingleHost(cluster));
            assertThat(stats).isNotNull();
            assertThat(stats.getMeasurementsCount()).isEqualTo(1);
            assertThat(stats.getLatencyScore()).isNotEqualTo(-1);
        } finally {
            cluster.close();
        }
    }

    @Test(groups = "short")
    public void should_discard_latency_when_unavailable() throws Exception {
        // given
        String query = "SELECT foo FROM bar";
        primingClient.prime(
                queryBuilder()
                        .withQuery(query)
                        .withThen(then().withResult(unavailable))
                        .build()
        );
        LatencyAwarePolicy latencyAwarePolicy = LatencyAwarePolicy.builder(new RoundRobinPolicy())
                .withMininumMeasurements(1)
                .build();
        Cluster.Builder builder = super.createClusterBuilder();
        builder.withLoadBalancingPolicy(latencyAwarePolicy);
        Cluster cluster = builder.build();
        try {
            cluster.init(); // force initialization of latency aware policy
            LatencyTrackerBarrier barrier = new LatencyTrackerBarrier(1);
            cluster.register(barrier);
            Session session = cluster.connect();
            // when
            try {
                session.execute(query);
                fail("Should have thrown NoHostAvailableException");
            } catch (NoHostAvailableException e) {
                // ok
                Throwable error = e.getErrors().get(hostAddress);
                assertThat(error).isNotNull();
                assertThat(error).isInstanceOf(UnavailableException.class);
            }
            // then
            // wait until trackers have been notified
            barrier.await();
            // make sure the updater is called at least once
            latencyAwarePolicy.new Updater().run();
            LatencyAwarePolicy.Snapshot snapshot = latencyAwarePolicy.getScoresSnapshot();
            assertThat(snapshot.getAllStats()).isEmpty();
            LatencyAwarePolicy.Snapshot.Stats stats = snapshot.getStats(retrieveSingleHost(cluster));
            assertThat(stats).isNull();
        } finally {
            cluster.close();
        }
    }

    @Test(groups = "short")
    public void should_consider_latency_when_read_timeout() throws Exception {
        String query = "SELECT foo FROM bar";
        primingClient.prime(
                queryBuilder()
                        .withQuery(query)
                        .withThen(then().withResult(read_request_timeout))
                        .build()
        );

        LatencyAwarePolicy latencyAwarePolicy = LatencyAwarePolicy.builder(new RoundRobinPolicy())
                .withMininumMeasurements(1)
                .build();
        Cluster.Builder builder = super.createClusterBuilder();
        builder.withLoadBalancingPolicy(latencyAwarePolicy);
        builder.withRetryPolicy(FallthroughRetryPolicy.INSTANCE);
        Cluster cluster = builder.build();
        try {
            cluster.init(); // force initialization of latency aware policy
            LatencyTrackerBarrier barrier = new LatencyTrackerBarrier(1);
            cluster.register(barrier);
            Session session = cluster.connect();
            // when
            try {
                session.execute(query);
                fail("Should have thrown ReadTimeoutException");
            } catch (ReadTimeoutException e) {
                // ok
            }
            // then
            // wait until trackers have been notified
            barrier.await();
            // make sure the updater is called at least once
            latencyAwarePolicy.new Updater().run();
            LatencyAwarePolicy.Snapshot snapshot = latencyAwarePolicy.getScoresSnapshot();
            assertThat(snapshot.getAllStats()).hasSize(1);
            LatencyAwarePolicy.Snapshot.Stats stats = snapshot.getStats(retrieveSingleHost(cluster));
            assertThat(stats).isNotNull();
            assertThat(stats.getMeasurementsCount()).isEqualTo(1);
            assertThat(stats.getLatencyScore()).isNotEqualTo(-1);
        } finally {
            cluster.close();
        }
    }

}
