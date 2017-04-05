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

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AsyncQueryTest extends CCMTestsSupport {

    Logger logger = LoggerFactory.getLogger(AsyncQueryTest.class);

    @DataProvider(name = "keyspace")
    public static Object[][] keyspace() {
        return new Object[][]{{"asyncquerytest"}, {"\"AsyncQueryTest\""}};
    }

    @Override
    public void onTestContextInitialized() {
        for (Object[] objects : keyspace()) {
            String keyspace = (String) objects[0];
            execute(
                    String.format("create keyspace %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace),
                    String.format("create table %s.foo(k int, v int, primary key (k, v))", keyspace));
            for (int v = 1; v <= 100; v++)
                execute(String.format("insert into %s.foo (k, v) values (1, %d)", keyspace, v));
        }
    }

    /**
     * Checks that a cancelled query releases the connection (JAVA-407).
     */
    @Test(groups = "short")
    public void cancelled_query_should_release_the_connection() throws InterruptedException {
        ResultSetFuture future = session().executeAsync("select release_version from system.local");
        future.cancel(true);
        assertTrue(future.isCancelled());

        TimeUnit.MILLISECONDS.sleep(100);

        HostConnectionPool pool = getPool(session());
        for (Connection connection : pool.connections) {
            assertEquals(connection.inFlight.get(), 0);
        }
    }

    @Test(groups = "short")
    public void should_init_cluster_and_session_if_needed() throws Exception {
        // For this test we need an uninitialized cluster, so we can't reuse the one provided by the
        // parent class. Rebuild a new one with the same (unique) host.
        Host host = cluster().getMetadata().allHosts().iterator().next();

        Cluster cluster2 = register(Cluster.builder()
                .addContactPointsWithPorts(Lists.newArrayList(host.getSocketAddress()))
                .build());
        Session session2 = cluster2.newSession();

        // Neither cluster2 nor session2 are initialized at this point
        assertThat(cluster2.manager.metadata).isNull();

        ResultSetFuture future = session2.executeAsync("select release_version from system.local");
        Row row = Uninterruptibles.getUninterruptibly(future).one();

        assertThat(row.getString(0)).isNotEmpty();
    }

    @Test(groups = "short", dataProvider = "keyspace", enabled = false,
            description = "disabled because the blocking USE call in the current pool implementation makes it deadlock")
    public void should_chain_query_on_async_session_init_with_same_executor(String keyspace) throws Exception {
        ListenableFuture<Integer> resultFuture = connectAndQuery(keyspace, GuavaCompatibility.INSTANCE.sameThreadExecutor());

        Integer result = Uninterruptibles.getUninterruptibly(resultFuture);
        assertThat(result).isEqualTo(1);
    }

    @Test(groups = "short", dataProvider = "keyspace")
    public void should_chain_query_on_async_session_init_with_different_executor(String keyspace) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(1);

        ListenableFuture<Integer> resultFuture = connectAndQuery(keyspace, executor);

        Integer result = Uninterruptibles.getUninterruptibly(resultFuture);
        assertThat(result).isEqualTo(1);

        executor.shutdownNow();
    }

    @Test(groups = "short")
    public void should_propagate_error_to_chained_query_if_session_init_fails() throws Exception {
        ListenableFuture<Integer> resultFuture = connectAndQuery("wrong_keyspace", GuavaCompatibility.INSTANCE.sameThreadExecutor());

        try {
            Uninterruptibles.getUninterruptibly(resultFuture);
        } catch (ExecutionException e) {
            assertThat(e.getCause())
                    .isInstanceOf(InvalidQueryException.class)
                    .hasMessage("Keyspace 'wrong_keyspace' does not exist");
        }
    }

    @Test(groups = "short")
    public void should_fail_when_synchronous_call_on_io_thread() throws Exception {
        for (int i = 0; i < 1000; i++) {
            ResultSetFuture f = session().executeAsync("select release_version from system.local");
            ListenableFuture<Thread> f2 = Futures.transform(f, new Function<ResultSet, Thread>() {
                @Override
                public Thread apply(ResultSet input) {
                    session().execute("select release_version from system.local");
                    return Thread.currentThread();
                }
            });
            if (isFailed(f2, IllegalStateException.class, "Detected a synchronous call on an I/O thread")) {
                return;
            }
        }
        fail("callback was not executed on io thread in 1000 attempts, something may be wrong.");
    }

    @Test(groups = "short")
    public void should_fail_when_synchronous_call_on_io_thread_with_session_wrapper() throws Exception {
        final Session session = new SessionWrapper(session());
        for (int i = 0; i < 1000; i++) {
            ResultSetFuture f = session.executeAsync("select release_version from system.local");
            ListenableFuture<Thread> f2 = Futures.transform(f, new Function<ResultSet, Thread>() {
                @Override
                public Thread apply(ResultSet input) {
                    session.execute("select release_version from system.local");
                    return Thread.currentThread();
                }
            });
            if (isFailed(f2, IllegalStateException.class, "Detected a synchronous call on an I/O thread")) {
                return;
            }
        }
        fail("callback was not executed on io thread in 1000 attempts, something may be wrong.");
    }

    @Test(groups = "short")
    @CassandraVersion(value = "2.0.0", description = "Paging is not supported until 2.0")
    public void should_fail_when_auto_paging_on_io_thread() throws Exception {
        for (int i = 0; i < 1000; i++) {
            Statement statement = new SimpleStatement("select v from asyncquerytest.foo where k = 1");
            // Ensure results will be paged (there are 100 rows in the test table)
            statement.setFetchSize(10);
            ResultSetFuture f = session().executeAsync(statement);
            ListenableFuture<Thread> f2 = Futures.transform(f, new Function<ResultSet, Thread>() {
                @Override
                public Thread apply(ResultSet rs) {
                    rs.all(); // Consume the whole result set
                    return Thread.currentThread();
                }
            });
            if (isFailed(f2, IllegalStateException.class, "Detected a synchronous call on an I/O thread")) {
                return;
            }
        }
        fail("callback was not executed on io thread in 1000 attempts, something may be wrong.");
    }

    private boolean isFailed(ListenableFuture<Thread> future, Class<?> expectedException, String expectedMessage) {
        try {
            Thread executedThread = future.get();
            if (executedThread != Thread.currentThread()) {
                fail("Expected a failed future, callback was executed on " + executedThread);
            } else {
                // Callback was invoked on the same thread, which indicates that the future completed
                // before the transform callback was registered. Try again to produce case where callback
                // is called on io thread.
                logger.warn("Future completed before transform callback registered, will try again.");
            }
        } catch (Exception e) {
            assertThat(Throwables.getRootCause(e))
                    .isInstanceOf(expectedException)
                    .hasMessageContaining(expectedMessage);
            return true;
        }
        return false;
    }

    private ListenableFuture<Integer> connectAndQuery(String keyspace, Executor executor) {
        ListenableFuture<Session> sessionFuture = cluster().connectAsync(keyspace);
        ListenableFuture<ResultSet> queryFuture = GuavaCompatibility.INSTANCE.transformAsync(sessionFuture, new AsyncFunction<Session, ResultSet>() {
            @Override
            public ListenableFuture<ResultSet> apply(Session session) throws Exception {
                return session.executeAsync("select v from foo where k = 1");
            }
        }, executor);
        return Futures.transform(queryFuture, new Function<ResultSet, Integer>() {
            @Override
            public Integer apply(ResultSet rs) {
                return rs.one().getInt(0);
            }
        }, executor);
    }

    private static HostConnectionPool getPool(Session session) {
        Collection<HostConnectionPool> pools = ((SessionManager) session).pools.values();
        assertEquals(pools.size(), 1);
        return pools.iterator().next();
    }

    private static class SessionWrapper extends AbstractSession {

        private final Session session;

        public SessionWrapper(Session session) {
            this.session = session;
        }

        @Override
        public ResultSet execute(Statement statement) {
            // test a custom call to checkNotInEventLoop()
            checkNotInEventLoop();
            return executeAsync(statement).getUninterruptibly();
        }

        @Override
        public String getLoggedKeyspace() {
            return session.getLoggedKeyspace();
        }

        @Override
        public Session init() {
            return session.init();
        }

        @Override
        public ListenableFuture<Session> initAsync() {
            return session.initAsync();
        }

        @Override
        public ResultSetFuture executeAsync(Statement statement) {
            return session.executeAsync(statement);
        }

        @Override
        public CloseFuture closeAsync() {
            return session.closeAsync();
        }

        @Override
        public boolean isClosed() {
            return session.isClosed();
        }

        @Override
        public Cluster getCluster() {
            return session.getCluster();
        }

        @Override
        public State getState() {
            return session.getState();
        }

        @Override
        protected ListenableFuture<PreparedStatement> prepareAsync(String query, Map<String, ByteBuffer> customPayload) {
            return ((SessionManager) session).prepareAsync(query, customPayload);
        }
    }
}
