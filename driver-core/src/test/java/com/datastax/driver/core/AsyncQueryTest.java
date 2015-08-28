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
package com.datastax.driver.core;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datastax.driver.core.exceptions.InvalidQueryException;

public class AsyncQueryTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(
            "create table foo(k int primary key, v int)",
            "insert into foo (k, v) values (1, 1)"
        );
    }

    /**
     * Checks that a cancelled query releases the connection (JAVA-407).
     */
    @Test(groups = "short")
    public void cancelled_query_should_release_the_connection() throws InterruptedException {
        ResultSetFuture future = session.executeAsync("select release_version from system.local");
        future.cancel(true);
        assertTrue(future.isCancelled());

        TimeUnit.MILLISECONDS.sleep(100);

        HostConnectionPool pool = getPool(session);
        for (Connection connection : pool.connections) {
            assertEquals(connection.inFlight.get(), 0);
        }
    }

    @Test(groups = "short")
    public void should_init_cluster_and_session_if_needed() throws Exception {
        // For this test we need an uninitialized cluster, so we can't reuse the one provided by the
        // parent class. Rebuild a new one with the same (unique) host.
        Host host = cluster.getMetadata().allHosts().iterator().next();

        Cluster cluster2 = null;
        try {
            cluster2 = Cluster.builder()
                .addContactPointsWithPorts(Lists.newArrayList(host.getSocketAddress()))
                .build();
            Session session2 = cluster2.newSession();

            // Neither cluster2 nor session2 are initialized at this point
            assertThat(cluster2.manager.metadata).isNull();

            ResultSetFuture future = session2.executeAsync(String.format("select v from %s.foo where k = 1", keyspace));
            Row row = Uninterruptibles.getUninterruptibly(future).one();

            assertThat(row.getInt(0)).isEqualTo(1);
        } finally {
            if (cluster2 != null)
                cluster2.close();
        }
    }

    @Test(groups = "short")
    public void should_chain_query_on_async_session_init() throws Exception {
        ListenableFuture<Integer> resultFuture = connectAndQuery(this.keyspace);

        Integer result = Uninterruptibles.getUninterruptibly(resultFuture);
        assertThat(result).isEqualTo(1);
    }

    @Test(groups = "short")
    public void should_propagate_error_to_chained_query_if_session_init_fails() throws Exception {
        ListenableFuture<Integer> resultFuture = connectAndQuery("wrong_keyspace");

        try {
            Uninterruptibles.getUninterruptibly(resultFuture);
        } catch (ExecutionException e) {
            assertThat(e.getCause())
                .isInstanceOf(InvalidQueryException.class)
                .hasMessage("Keyspace 'wrong_keyspace' does not exist");
        }
    }

    private ListenableFuture<Integer> connectAndQuery(String keyspace) {
        ListenableFuture<Session> sessionFuture = cluster.connectAsync(keyspace);
        ListenableFuture<ResultSet> queryFuture = Futures.transform(sessionFuture, new AsyncFunction<Session, ResultSet>() {
            @Override
            public ListenableFuture<ResultSet> apply(Session session) throws Exception {
                return session.executeAsync("select v from foo where k = 1");
            }
        });
        return Futures.transform(queryFuture, new Function<ResultSet, Integer>() {
            @Override
            public Integer apply(ResultSet rs) {
                return rs.one().getInt(0);
            }
        });
    }

    private static HostConnectionPool getPool(Session session) {
        Collection<HostConnectionPool> pools = ((SessionManager)session).pools.values();
        assertEquals(pools.size(), 1);
        return pools.iterator().next();
    }
}
