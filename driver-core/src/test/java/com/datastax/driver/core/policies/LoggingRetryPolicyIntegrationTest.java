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

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.ServerError;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.slf4j.helpers.MessageFormatter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE;
import static com.datastax.driver.core.WriteType.SIMPLE;
import static com.datastax.driver.core.policies.LoggingRetryPolicy.*;
import static com.datastax.driver.core.policies.RetryPolicy.RetryDecision.ignore;
import static com.datastax.driver.core.policies.RetryPolicy.RetryDecision.tryNextHost;
import static org.apache.log4j.Level.INFO;
import static org.scassandra.http.client.Result.*;

/**
 * Integration tests with LoggingRetryPolicy.
 */
public class LoggingRetryPolicyIntegrationTest extends AbstractRetryPolicyIntegrationTest {

    private volatile RetryPolicy.RetryDecision retryDecision;

    private Logger logger = Logger.getLogger(LoggingRetryPolicy.class.getName());

    private MemoryAppender appender;

    private Level originalLevel;
    private ConsistencyLevel defaultCL;

    @BeforeClass(groups = {"short", "unit"})
    public void setUpRetryPolicy() {
        setRetryPolicy(new LoggingRetryPolicy(new CustomRetryPolicy()));
    }

    @BeforeMethod(groups = {"short", "unit"})
    public void storeDefaultCL() {
        defaultCL = cluster.getConfiguration().getQueryOptions().getConsistencyLevel();
    }

    @BeforeMethod(groups = {"short", "unit"})
    public void startCapturingLogs() {
        originalLevel = logger.getLevel();
        logger.setLevel(INFO);
        logger.addAppender(appender = new MemoryAppender());
    }

    @AfterMethod(groups = {"short", "unit"})
    public void stopCapturingLogs() {
        logger.setLevel(originalLevel);
        logger.removeAppender(appender);
    }

    @Test(groups = "short")
    public void should_log_ignored_read_timeout() throws InterruptedException {
        simulateError(1, read_request_timeout);
        retryDecision = ignore();
        query();
        String line = appender.waitAndGet(5000);
        assertThat(line.trim()).isEqualTo(expectedMessage(IGNORING_READ_TIMEOUT, defaultCL, 1, 0, false, 0));
    }

    @Test(groups = "short")
    public void should_log_retried_read_timeout() throws InterruptedException {
        simulateError(1, read_request_timeout);
        retryDecision = tryNextHost(LOCAL_ONE);
        query();
        String line = appender.waitAndGet(5000);
        assertThat(line.trim()).isEqualTo(expectedMessage(RETRYING_ON_READ_TIMEOUT, "next host", LOCAL_ONE, defaultCL, 1, 0, false, 0));
    }

    @Test(groups = "short")
    public void should_log_ignored_write_timeout() throws InterruptedException {
        simulateError(1, write_request_timeout);
        retryDecision = ignore();
        query();
        String line = appender.waitAndGet(5000);
        assertThat(line.trim()).isEqualTo(expectedMessage(IGNORING_WRITE_TIMEOUT, defaultCL, SIMPLE, 1, 0, 0));
    }

    @Test(groups = "short")
    public void should_log_retried_write_timeout() throws InterruptedException {
        simulateError(1, write_request_timeout);
        retryDecision = tryNextHost(LOCAL_ONE);
        query();
        String line = appender.waitAndGet(5000);
        assertThat(line.trim()).isEqualTo(expectedMessage(RETRYING_ON_WRITE_TIMEOUT, "next host", LOCAL_ONE, defaultCL, SIMPLE, 1, 0, 0));
    }

    @Test(groups = "short")
    public void should_log_ignored_unavailabe() throws InterruptedException {
        simulateError(1, unavailable);
        retryDecision = ignore();
        query();
        String line = appender.waitAndGet(5000);
        assertThat(line.trim()).isEqualTo(expectedMessage(IGNORING_UNAVAILABLE, defaultCL, 1, 0, 0));
    }

    @Test(groups = "short")
    public void should_log_retried_unavailable() throws InterruptedException {
        simulateError(1, unavailable);
        retryDecision = tryNextHost(LOCAL_ONE);
        query();
        String line = appender.waitAndGet(5000);
        assertThat(line.trim()).isEqualTo(expectedMessage(RETRYING_ON_UNAVAILABLE, "next host", LOCAL_ONE, defaultCL, 1, 0, 0));
    }

    @Test(groups = "short")
    public void should_log_ignored_request_error() throws InterruptedException {
        simulateError(1, server_error);
        retryDecision = ignore();
        query();
        String line = appender.waitAndGet(5000);
        assertThat(line.trim()).isEqualTo(expectedMessage(IGNORING_REQUEST_ERROR, defaultCL, 0, new ServerError(host1.getSocketAddress(), "Server Error")));
    }

    @Test(groups = "short")
    public void should_log_retried_request_error() throws InterruptedException {
        simulateError(1, server_error);
        retryDecision = tryNextHost(LOCAL_ONE);
        query();
        String line = appender.waitAndGet(5000);
        assertThat(line.trim()).isEqualTo(expectedMessage(RETRYING_ON_REQUEST_ERROR, "next host", LOCAL_ONE, defaultCL, 0, new ServerError(host1.getSocketAddress(), "Server Error")));
    }

    private String expectedMessage(String template, Object... args) {
        return MessageFormatter.arrayFormat(template, args).getMessage();
    }

    /**
     * Dynamically modifiable retry policy.
     */
    class CustomRetryPolicy implements RetryPolicy {

        @Override
        public RetryPolicy.RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
            return retryDecision;
        }

        @Override
        public RetryPolicy.RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
            return retryDecision;
        }

        @Override
        public RetryPolicy.RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
            return retryDecision;
        }

        @Override
        public RetryPolicy.RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
            return retryDecision;
        }

        @Override
        public void init(Cluster cluster) {
            // nothing to do
        }

        @Override
        public void close() {
            // nothing to do
        }

    }
}
