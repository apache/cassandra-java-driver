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
import org.scassandra.http.client.Result;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.scassandra.http.client.PrimingRequest.queryBuilder;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.Result.*;

public class ErrorAwarePolicyIntegrationTest {

    private QueryTracker queryTracker;
    private Clock clock;
    private ScassandraCluster sCluster;
    private AtomicInteger errorCounter;
    private LatencyTracker latencyTracker;

    @BeforeMethod(groups = "short")
    public void setUp() {
        queryTracker = new QueryTracker();
        clock = mock(Clock.class);
        sCluster = ScassandraCluster.builder().withNodes(2).build();
        sCluster.init();
        errorCounter = new AtomicInteger(0);
        latencyTracker = new LatencyTracker() {
            @Override
            public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
                if (exception != null)
                    errorCounter.incrementAndGet();
            }

            @Override
            public void onRegister(Cluster cluster) {
            }

            @Override
            public void onUnregister(Cluster cluster) {
            }
        };
        // By default node 1 should always fail, 2 succeed.
        prime(1, unauthorized);
        prime(2, success);
    }

    @AfterMethod(groups = "short")
    public void tearDown() {
        sCluster.stop();
    }

    private Cluster.Builder builder(LoadBalancingPolicy lbp) {
        return Cluster.builder()
                .withNettyOptions(nonQuietClusterCloseOptions)
                .addContactPoints(sCluster.address(1).getAddress())
                .withPort(sCluster.getBinaryPort())
                .withLoadBalancingPolicy(lbp);
    }

    private void prime(int node, Result result) {
        sCluster.node(node).primingClient().prime(
                queryBuilder()
                        .withQuery(QueryTracker.QUERY)
                        .withThen(then().withResult(result))
                        .build()
        );
    }

    /**
     * Checks that {@link LatencyTracker#update} was called at least expectedCount times within 5 seconds.
     * <p/>
     * Note that the usefulness of this is dependent on the {@link ErrorAwarePolicy.PerHostErrorTracker} being invoked
     * before the latency tracker being used to track error invocations in this test.  The existing implementation
     * seems to invoke update on latency trackers in order that they are registered.
     *
     * @param expectedCount Expected number of errors to have been invoked.
     */
    private void awaitTrackerUpdate(final int expectedCount) {
        ConditionChecker.check()
                .every(10)
                .that(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        return errorCounter.get() >= expectedCount;
                    }
                })
                .before(5000)
                .becomesTrue();
    }

    private void setTime(long time, TimeUnit timeUnit) {
        when(clock.nanoTime()).thenReturn(TimeUnit.NANOSECONDS.convert(time, timeUnit));
    }

    /**
     * Validates that {@link ErrorAwarePolicy} properly excludes a host after the maximum number of errors is exceeded.
     * <p/>
     * This test configures a maximum of 1 error per minute and executes 2 failing queries against host1 during the
     * first 5 simulated seconds. host1 should be excluded as soon as the rolling count updates over the next 5-second
     * interval.
     * <p/>
     * It then makes another query and ensures it is executed against host2 and that the response was successful.
     *
     * @jira_ticket JAVA-1055
     * @test_category load_balancing:error_aware
     * @since 3.1.0
     */
    @Test(groups = "short")
    public void should_exclude_host_after_reaching_maximum_errors() throws InterruptedException {
        LoadBalancingPolicy lbp = ErrorAwarePolicy.builder(new SortingLoadBalancingPolicy())
                .withMaxErrorsPerMinute(1)
                .withClock(clock)
                .build();

        Cluster cluster = builder(lbp).build();

        try {
            Session session = cluster.connect();
            cluster.register(latencyTracker);

            setTime(0, SECONDS);
            // Make 2 queries producing a count higher than the threshold
            queryTracker.query(session, 2, UnauthorizedException.class, sCluster.address(1));
            awaitTrackerUpdate(2);

            // Advance time so that RollingCount ticks and updates its count.
            setTime(5, SECONDS);

            // The next query should succeed and hit node 2 since node 1 is now ignored.
            queryTracker.query(session, 1, sCluster.address(2));
        } finally {
            cluster.close();
        }
    }

    /**
     * Validates that {@link ErrorAwarePolicy} will include a previously excluded host after the configured retry
     * period has elapsed.
     * <p/>
     * The test executes queries with error to get host1 excluded.  It then executes queries over 70 simulated seconds
     * and then executes another query after this time has elapsed and ensures that the next query execution uses host1.
     *
     * @jira_ticket JAVA-1055
     * @test_category load_balancing:error_aware
     * @since 3.1.0
     */
    @Test(groups = "short")
    public void should_resurrect_host_after_retry_period() throws InterruptedException {
        LoadBalancingPolicy lbp = ErrorAwarePolicy.builder(new SortingLoadBalancingPolicy())
                .withMaxErrorsPerMinute(1)
                .withRetryPeriod(70, SECONDS)
                .withClock(clock)
                .build();

        Cluster cluster = builder(lbp).build();
        try {
            Session session = cluster.connect();
            cluster.register(latencyTracker);

            setTime(0, SECONDS);
            // Make 2 queries producing a count higher than the threshold
            queryTracker.query(session, 2, UnauthorizedException.class, sCluster.address(1));
            awaitTrackerUpdate(2);

            // Advance time so that RollingCount ticks and updates its count.
            setTime(5, SECONDS);

            // Execute some queries, these should all succeed and hit host2 since host1 is excluded.
            queryTracker.query(session, 5, sCluster.address(2));

            // Advance time after the retry period
            setTime(75, SECONDS);

            // At this the load balancing policy should resurrect node 1 which will be used and fail.
            queryTracker.query(session, 1, UnauthorizedException.class, sCluster.address(1));

        } finally {
            cluster.close();
        }
    }

    /**
     * Validates that {@link ErrorAwarePolicy} will not penalize errors that are not considered in the default
     * {@link ErrorAwarePolicy.ErrorFilter} implementation.
     * <p/>
     * Executes 10 queries with each error type and ensures that host1 is used each time, verifying that it was
     * never excluded.
     *
     * @jira_ticket JAVA-1055
     * @test_category load_balancing:error_aware
     * @since 3.1.0
     */
    @Test(groups = "short")
    public void should_not_penalize_default_ignored_exceptions() throws InterruptedException {
        LoadBalancingPolicy lbp = ErrorAwarePolicy.builder(new SortingLoadBalancingPolicy())
                .withMaxErrorsPerMinute(1)
                .withClock(clock)
                .build();

        // Use fall through retry policy so other hosts aren't tried.
        Cluster cluster = builder(lbp).withRetryPolicy(FallthroughRetryPolicy.INSTANCE).build();
        try {
            Session session = cluster.connect();
            cluster.register(latencyTracker);

            setTime(0, SECONDS);
            // TODO: Add Read and Write Failure, FunctionExecution exception when Scassandra supports v4.
            prime(1, read_request_timeout);
            queryTracker.query(session, 10, ReadTimeoutException.class, sCluster.address(1));
            awaitTrackerUpdate(10);

            setTime(5, SECONDS);

            prime(1, write_request_timeout);
            queryTracker.query(session, 10, WriteTimeoutException.class, sCluster.address(1));
            awaitTrackerUpdate(20);

            setTime(10, SECONDS);

            prime(1, unavailable);
            queryTracker.query(session, 10, UnavailableException.class, sCluster.address(1));
            awaitTrackerUpdate(30);

            setTime(15, SECONDS);

            prime(1, already_exists);
            queryTracker.query(session, 10, AlreadyExistsException.class, sCluster.address(1));
            awaitTrackerUpdate(40);

            setTime(20, SECONDS);

            prime(1, invalid);
            queryTracker.query(session, 10, InvalidQueryException.class, sCluster.address(1));
            awaitTrackerUpdate(50);

            setTime(25, SECONDS);

            prime(1, syntax_error);
            queryTracker.query(session, 10, SyntaxError.class, sCluster.address(1));
            awaitTrackerUpdate(60);

            setTime(30, SECONDS);

            // ensure host1 still used after another tick.
            queryTracker.query(session, 10, SyntaxError.class, sCluster.address(1));
        } finally {
            cluster.close();
        }
    }

    /**
     * Validates that {@link ErrorAwarePolicy} will regard a custom {@link ErrorAwarePolicy.ErrorFilter} by only
     * penalizing a node when it produces exceptions that evaluate to true in the filter implementation.
     * <p/>
     * It first executes 10 queries with an error type that is not considered and verify that host1 is never excluded.
     * <p/>
     * It then executes queries with an error type that is considered and verifies that host1 is then excluded and host2
     * is used instead.
     *
     * @jira_ticket JAVA-1055
     * @test_category load_balancing:error_aware
     * @since 3.1.0
     */
    @Test(groups = "short")
    public void should_only_consider_exceptions_based_on_errors_filter() throws InterruptedException {
        ErrorAwarePolicy.ErrorFilter iqeOnlyFilter = new ErrorAwarePolicy.ErrorFilter() {
            @Override
            public boolean shouldConsiderError(Exception e, Host host, Statement statement) {
                return e.getClass().isAssignableFrom(InvalidQueryException.class);
            }
        };

        LoadBalancingPolicy lbp = ErrorAwarePolicy.builder(new SortingLoadBalancingPolicy())
                .withMaxErrorsPerMinute(1)
                .withClock(clock)
                .withErrorsFilter(iqeOnlyFilter)
                .build();

        // Use fall through retry policy so other hosts aren't tried.
        Cluster cluster = builder(lbp).withRetryPolicy(FallthroughRetryPolicy.INSTANCE).build();
        try {
            Session session = cluster.connect();
            cluster.register(latencyTracker);

            setTime(0, SECONDS);
            // UnauthorizedException evaluates to false in the filter, so it should not be considered.
            prime(1, unauthorized);
            queryTracker.query(session, 10, UnauthorizedException.class, sCluster.address(1));
            awaitTrackerUpdate(10);

            setTime(5, SECONDS);
            // should still query host1
            queryTracker.query(session, 1, UnauthorizedException.class, sCluster.address(1));

            // InvalidQueryException evaluates to true, so it *should* be considered increment the count
            prime(1, invalid);
            queryTracker.query(session, 2, InvalidQueryException.class, sCluster.address(1));
            awaitTrackerUpdate(13);

            // Advance time so that the rolling count updates the next time we query.
            // The first errors that were considered were at t = 5 seconds and this is when the rolling count was
            // initialized, so we want to be at t > 5 + 5 for the rolling count to update
            setTime(10, SECONDS);

            // The next query should succeed and hit node 2 since node 1 is now ignored.
            queryTracker.query(session, 1, sCluster.address(2));
        } finally {
            cluster.close();
        }
    }

    /**
     * Validates that an {@link ErrorAwarePolicy} configured with its defaults behaves as documented, that being that
     * the maximum number of errors is 1 and the retry period is 120 seconds.
     *
     * @jira_ticket JAVA-1055
     * @test_category load_balancing:error_aware
     * @since 3.1.0
     */
    @Test(groups = "short")
    public void should_regard_defaults() throws InterruptedException {
        LoadBalancingPolicy lbp = ErrorAwarePolicy.builder(new SortingLoadBalancingPolicy())
                .withClock(clock)
                .build();

        Cluster cluster = builder(lbp).build();
        try {
            Session session = cluster.connect();
            cluster.register(latencyTracker);

            setTime(0, SECONDS);
            // Make 2 queries producing a count higher than the threshold
            queryTracker.query(session, 2, UnauthorizedException.class, sCluster.address(1));
            awaitTrackerUpdate(2);

            // Advance time so the rolling count ticks, next query should go to host 2
            setTime(5, SECONDS);
            queryTracker.query(session, 5, sCluster.address(2));

            // Advance clock 30 seconds, this is within the retry period so host 1 should still be excluded.
            setTime(35, SECONDS);
            queryTracker.query(session, 5, sCluster.address(2));

            // At this point 120 seconds have elapsed, the load balancing policy should see that the retry period has
            // elapsed and resurrect node 1 which will be used and fail.
            setTime(125, SECONDS);
            queryTracker.query(session, 1, UnauthorizedException.class, sCluster.address(1));

        } finally {
            cluster.close();
        }
    }
}
