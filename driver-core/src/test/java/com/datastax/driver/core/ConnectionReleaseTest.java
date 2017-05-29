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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import org.scassandra.http.client.PrimingRequest;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.testng.Assert.fail;

public class ConnectionReleaseTest extends ScassandraTestBase {

    /**
     * <p/>
     * Validates that when a future is set that the stream associated with the future's request is released.
     * This prevents situations where a user may not be specifying a separate executor on a callback/
     * transform to a ResultSetFuture, which is not recommended, causing executeAsync to block in borrowConnection
     * until stream ids become available.
     * <p/>
     * Executes the following:
     * <p/>
     * <ol>
     * <li>Sets # of connections per host to 1.</li>
     * <li>Sends MAX_STREAM_PER_CONNECTION-1 requests that take 10 seconds to execute.</li>
     * <li>Calls executeAsync to retrieve records from test1 with k=1.</li>
     * <li>Transforms executeAsync to take the 'c' column from the result and query test2.
     * This is done without an executor to ensure the netty worker is used and has to wait for the function
     * completion.</li>
     * <li>Asserts that the transformed future completes within pool timeout and the value is as expected.</li>
     * </ol>
     *
     * @jira_ticket JAVA-666
     * @expected_result Are able to transform a Future without hanging in executeAsync as connection should be freed
     * before the transform function is called.
     * @test_category queries:async
     * @since 2.0.10, 2.1.6
     */
    @SuppressWarnings("unchecked")
    @Test(groups = "short")
    public void should_release_connection_before_completing_future() throws Exception {
        Cluster cluster = null;
        Collection<ResultSetFuture> mockFutures = Lists.newArrayList();
        try {
            primingClient.prime(
                    PrimingRequest.queryBuilder()
                            .withQuery("mock query")
                            .withThen(then().withRows(ImmutableMap.of("key", 1))
                                    .withFixedDelay(10000L))
                            .build()
            );
            primingClient.prime(
                    PrimingRequest.queryBuilder()
                            .withQuery("select c from test1 where k=1")
                            .withThen(then().withRows(ImmutableMap.of("c", "hello")))
                            .build()
            );
            primingClient.prime(
                    PrimingRequest.queryBuilder()
                            .withQuery("select n from test2 where c='hello'")
                            .withThen(then().withRows(ImmutableMap.of("n", "world")))
                            .build()
            );

            cluster = Cluster.builder()
                    .addContactPoints(hostAddress.getAddress())
                    .withPort(scassandra.getBinaryPort())
                    .withPoolingOptions(new PoolingOptions()
                            .setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
                            .setMaxConnectionsPerHost(HostDistance.LOCAL, 1))
                    .build();

            final Session session = cluster.connect("ks");
            // Consume all stream ids except one.
            for (int i = 0; i < StreamIdGenerator.MAX_STREAM_PER_CONNECTION_V2 - 1; i++)
                mockFutures.add(session.executeAsync("mock query"));


            ListenableFuture<ResultSet> future = GuavaCompatibility.INSTANCE.transformAsync(session.executeAsync("select c from test1 where k=1"),
                    new AsyncFunction<ResultSet, ResultSet>() {
                        @Override
                        public ListenableFuture<ResultSet> apply(ResultSet result) {
                            Row row = result.one();
                            String c = row.getString("c");
                            // Execute async might hang if no streams are available.  This happens if the connection
                            // was not release.
                            return session.executeAsync("select n from test2 where c='" + c + "'");
                        }
                    });

            long waitTimeInMs = 2000;
            try {
                ResultSet result = future.get(waitTimeInMs, TimeUnit.MILLISECONDS);
                assertThat(result.one().getString("n")).isEqualTo("world");
            } catch (TimeoutException e) {
                fail("Future timed out after " + waitTimeInMs + "ms.  " +
                        "There is a strong possibility connection is not being released.");
            }
        } finally {
            // Cancel all pending requests.
            for (ResultSetFuture future : mockFutures)
                future.cancel(true);
            if (cluster != null)
                cluster.close();
        }
    }
}
