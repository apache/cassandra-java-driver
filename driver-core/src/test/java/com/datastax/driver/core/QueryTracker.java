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

import com.datastax.driver.core.exceptions.CoordinatorException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.assertj.core.util.Maps;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.fail;

/**
 * A convenience utility for executing queries against a {@link Session} and tracking
 * which hosts were queried.
 */
public class QueryTracker {
    public static final String QUERY = "select * from test.foo";

    Map<InetAddress, Integer> coordinators = Maps.newConcurrentHashMap();

    public void query(Session session, int times) {
        query(session, times, ConsistencyLevel.ONE);
    }

    public void query(Session session, int times, ConsistencyLevel cl) {
        query(session, times, cl, null);
    }

    public void query(Session session, int times, InetSocketAddress expectedHost) {
        query(session, times, new SimpleStatement(QUERY), null, expectedHost);
    }

    public void query(Session session, int times, Class<? extends Exception> expectedException, InetSocketAddress expectedHost) {
        query(session, times, new SimpleStatement(QUERY), expectedException, expectedHost);
    }

    public void query(Session session, int times, ConsistencyLevel cl, Class<? extends Exception> expectedException) {
        Statement statement = new SimpleStatement(QUERY);
        if (cl != null) {
            statement.setConsistencyLevel(cl);
        }

        query(session, times, statement, expectedException, null);
    }

    public void query(Session session, int times, Statement statement) {
        query(session, times, statement, null, null);
    }

    public void query(Session session, int times, Statement statement, Class<? extends Exception> expectedException, InetSocketAddress expectedHost) {
        List<ListenableFuture<ResultSet>> futures = newArrayList();

        for (int i = 0; i < times; i++) {
            futures.add(session.executeAsync(statement));
        }

        try {
            for (ListenableFuture<ResultSet> future : futures) {
                try {
                    ResultSet result = Uninterruptibles.getUninterruptibly(future, 1, TimeUnit.SECONDS);
                    InetSocketAddress address = result.getExecutionInfo().getQueriedHost().getSocketAddress();
                    InetAddress coordinator = address.getAddress();
                    Integer n = coordinators.get(coordinator);
                    coordinators.put(coordinator, n == null ? 1 : n + 1);
                    if (expectedHost != null) {
                        assertThat(address).isEqualTo(expectedHost);
                    }
                } catch (ExecutionException ex) {
                    Throwable cause = ex.getCause();
                    if (expectedException == null) {
                        fail("Query fail", ex);
                    } else {
                        assertThat(cause).isInstanceOf(expectedException);
                    }

                    if (cause instanceof CoordinatorException) {
                        assertThat(((CoordinatorException) cause).getAddress()).isEqualTo(expectedHost);
                    }
                }
            }
        } catch (Exception e) {
            fail("Queries failed", e);
        }
    }

    public int queryCount(ScassandraCluster sCluster, int dc, int node) {
        try {
            InetSocketAddress host = sCluster.address(dc, node);
            Integer queried = coordinators.get(host.getAddress());
            return queried != null ? queried : 0;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void assertQueried(ScassandraCluster sCluster, int dc, int node, int n) {
        int queryCount = queryCount(sCluster, dc, node);
        assertThat(queryCount)
                .as("Expected node %d:%d to be queried %d times but was %d", dc, node, n, queryCount)
                .isEqualTo(n);
    }

    public void reset() {
        this.coordinators.clear();
    }
}
