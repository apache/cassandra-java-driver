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

import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.log4j.Level.DEBUG;
import static org.apache.log4j.Level.INFO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.scassandra.http.client.PrimingRequest.Result.read_request_timeout;
import static org.scassandra.http.client.PrimingRequest.Result.unavailable;
import static org.scassandra.http.client.PrimingRequest.Result.write_request_timeout;
import static org.scassandra.http.client.PrimingRequest.queryBuilder;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;

import static com.datastax.driver.core.QueryLogger.builder;

/**
 * Tests for {@link QueryLogger} using Scassandra.
 * Contains only tests for exceptions (client timeout, read timeout, unavailable...);
 * other tests can be found in {@link QueryLoggerTest}.
 */
public class QueryLoggerErrorsTest extends ScassandraTestBase.PerClassCluster {

    private Logger slow = Logger.getLogger(QueryLogger.SLOW_LOGGER.getName());
    private Logger error = Logger.getLogger(QueryLogger.ERROR_LOGGER.getName());

    private MemoryAppender slowAppender;
    private MemoryAppender errorAppender;

    private QueryLogger queryLogger;

    @BeforeMethod(groups = { "short", "unit" })
    public void startCapturingLogs() {
        slow.addAppender(slowAppender = new MemoryAppender());
        error.addAppender(errorAppender = new MemoryAppender());
    }

    @AfterMethod(groups = { "short", "unit" })
    public void stopCapturingLogs() {
        slow.setLevel(null);
        error.setLevel(null);
        slow.removeAppender(slowAppender);
        error.removeAppender(errorAppender);
    }

    @BeforeMethod(groups = { "short", "unit" })
    @AfterMethod(groups = { "short", "unit" })
    public void resetLogLevels() {
        slow.setLevel(INFO);
        error.setLevel(INFO);
    }

    @BeforeMethod(groups = { "short", "unit" })
    public void resetQueryLogger() {
        queryLogger = null;
    }

    @AfterMethod(groups = { "short", "unit" })
    public void unregisterQueryLogger() {
        if(cluster != null && queryLogger != null) {
            cluster.unregister(queryLogger);
        }
    }

    @Test(groups = "short")
    public void should_log_slow_queries() throws Exception {
        // given
        slow.setLevel(DEBUG);
        queryLogger = builder()
            .withConstantThreshold(10)
            .build();
        cluster.register(queryLogger);
        String query = "SELECT foo FROM bar";
        primingClient.prime(
            queryBuilder()
                .withQuery(query)
                .withFixedDelay(100)
                .build()
        );
        // when
        session.execute(query);
        // then
        String line = slowAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query too slow")
            .contains("127.0.0.1")
            .contains(query);
    }

    @Test(groups = "short")
    public void should_log_timed_out_queries() throws Exception {
        // given
        error.setLevel(DEBUG);
        queryLogger = builder().build();
        cluster.register(queryLogger);
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(1);
        String query = "SELECT foo FROM bar";
        primingClient.prime(
            queryBuilder()
                .withQuery(query)
                .withFixedDelay(100)
                .build()
        );
        // when
        try {
            session.execute(query);
            fail("Should have thrown NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            // ok
        }
        // then
        String line = errorAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query error")
            .contains("127.0.0.1")
            .contains(Integer.toString(scassandra.getBinaryPort()))
            .contains(query)
            .contains(OperationTimedOutException.class.getName());
    }

    @Test(groups = "short")
    public void should_log_read_timeout_errors() throws Exception {
        // given
        error.setLevel(DEBUG);
        queryLogger = builder().build();
        cluster.register(queryLogger);
        String query = "SELECT foo FROM bar";
        primingClient.prime(
            queryBuilder()
                .withQuery(query)
                .withResult(read_request_timeout)
                .build()
        );
        // when
        try {
            session.execute(query);
            fail("Should have thrown ReadTimeoutException");
        } catch (ReadTimeoutException e) {
            // ok
        }
        // then
        String line = errorAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query error")
            .contains("127.0.0.1")
            .contains(Integer.toString(scassandra.getBinaryPort()))
            .contains(query)
            .contains(ReadTimeoutException.class.getName());
    }

    @Test(groups = "short")
    public void should_log_write_timeout_errors() throws Exception {
        // given
        error.setLevel(DEBUG);
        queryLogger = builder().build();
        cluster.register(queryLogger);
        String query = "UPDATE test SET foo = 'bar' where qix = ?";
        primingClient.prime(
            queryBuilder()
                .withQuery(query)
                .withResult(write_request_timeout)
                .build()
        );
        // when
        try {
            session.execute(query);
            fail("Should have thrown WriteTimeoutException");
        } catch (WriteTimeoutException e) {
            // ok
        }
        // then
        String line = errorAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query error")
            .contains("127.0.0.1")
            .contains(Integer.toString(scassandra.getBinaryPort()))
            .contains(query)
            .contains(WriteTimeoutException.class.getName());
    }

    @Test(groups = "short")
    public void should_log_unavailable_errors() throws Exception {
        // given
        error.setLevel(DEBUG);
        queryLogger = builder().build();
        cluster.register(queryLogger);
        String query = "SELECT foo FROM bar";
        primingClient.prime(
            queryBuilder()
                .withQuery(query)
                .withResult(unavailable)
                .build()
        );
        // when
        try {
            session.execute(query);
            fail("Should have thrown UnavailableException");
        } catch (UnavailableException e) {
            // ok
        }
        // then
        String line = errorAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query error")
            .contains("127.0.0.1")
            .contains(Integer.toString(scassandra.getBinaryPort()))
            .contains(query)
            .contains(UnavailableException.class.getName());
    }

}