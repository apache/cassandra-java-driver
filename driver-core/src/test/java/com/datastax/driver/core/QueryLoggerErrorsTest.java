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

import com.datastax.driver.core.exceptions.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.scassandra.http.client.Result;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.datastax.driver.core.QueryLogger.builder;
import static org.apache.log4j.Level.DEBUG;
import static org.apache.log4j.Level.INFO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.scassandra.http.client.PrimingRequest.queryBuilder;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.Result.*;

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

    private Cluster cluster = null;
    private Session session = null;
    private QueryLogger queryLogger = null;
    private Level originalError;
    private Level originalSlow;

    @BeforeMethod(groups = {"short", "unit"})
    public void setUp() {
        originalSlow = slow.getLevel();
        originalError = error.getLevel();
        slow.setLevel(INFO);
        error.setLevel(INFO);
        slow.addAppender(slowAppender = new MemoryAppender());
        error.addAppender(errorAppender = new MemoryAppender());

        queryLogger = null;

        cluster = createClusterBuilder().build();
        session = cluster.connect();
    }

    @AfterMethod(groups = {"short", "unit"}, alwaysRun = true)
    public void tearDown() {
        slow.setLevel(originalSlow);
        error.setLevel(originalError);
        slow.removeAppender(slowAppender);
        error.removeAppender(errorAppender);

        queryLogger = null;

        if (cluster != null) {
            cluster.close();
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
                        .withThen(then().withFixedDelay(100L))
                        .build()
        );
        // when
        session.execute(query);
        // then
        String line = slowAppender.waitAndGet(5000);
        assertThat(line)
                .contains("Query too slow")
                .contains(ip)
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
                        .withThen(then().withFixedDelay(100L))
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
                .contains(ip)
                .contains(Integer.toString(scassandra.getBinaryPort()))
                .contains(query)
                .contains("Timed out waiting for server response");
    }

    @DataProvider(name = "errors")
    public static Object[][] createErrors() {
        return new Object[][]{
                {unavailable, NoHostAvailableException.class, UnavailableException.class},
                {write_request_timeout, WriteTimeoutException.class, WriteTimeoutException.class},
                {read_request_timeout, ReadTimeoutException.class, ReadTimeoutException.class},
                {server_error, NoHostAvailableException.class, ServerError.class},
                {protocol_error, ProtocolError.class, ProtocolError.class},
                {bad_credentials, AuthenticationException.class, AuthenticationException.class},
                {overloaded, NoHostAvailableException.class, OverloadedException.class},
                {is_bootstrapping, NoHostAvailableException.class, BootstrappingException.class},
                {truncate_error, TruncateException.class, TruncateException.class},
                {syntax_error, SyntaxError.class, SyntaxError.class},
                {invalid, InvalidQueryException.class, InvalidQueryException.class},
                {config_error, InvalidConfigurationInQueryException.class, InvalidConfigurationInQueryException.class},
                {already_exists, AlreadyExistsException.class, AlreadyExistsException.class},
                {unprepared, DriverInternalError.class, UnpreparedException.class}
        };
    }

    @Test(groups = "short", dataProvider = "errors")
    public void should_log_exception_from_the_given_result(Result result, Class<? extends Exception> expectedException, Class<? extends Exception> loggedException) throws Exception {
        // given
        error.setLevel(DEBUG);
        queryLogger = builder().build();
        cluster.register(queryLogger);
        String query = "SELECT foo FROM bar";
        primingClient.prime(
                queryBuilder()
                        .withQuery(query)
                        .withThen(then().withResult(result))
                        .build()
        );
        // when
        try {
            session.execute(query);
            fail("Should have thrown NoHostAvailableException");
        } catch (Exception e) {
            if (e instanceof NoHostAvailableException) {
                assertThat(expectedException).isEqualTo(NoHostAvailableException.class);
                Throwable error = ((NoHostAvailableException) e).getErrors().get(hostAddress);
                assertThat(error).isNotNull().isOfAnyClassIn(loggedException);
            } else {
                assertThat(e).isInstanceOf(expectedException);
            }
        }
        // then
        String line = errorAppender.waitAndGet(5000);
        assertThat(line)
                .contains("Query error")
                .contains(ip)
                .contains(Integer.toString(scassandra.getBinaryPort()))
                .contains(query)
                .contains(loggedException.getName());
    }

}
