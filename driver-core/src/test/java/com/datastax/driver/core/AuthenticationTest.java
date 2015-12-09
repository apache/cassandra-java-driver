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

import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests for authenticated cluster access
 */
public class AuthenticationTest {

    private static final Logger logger = LoggerFactory.getLogger(AuthenticationTest.class);

    private CCMBridge ccm;

    /**
     * creates a cluster and turns on password authentication before starting it.
     */
    @BeforeClass(groups = "short")
    public void setupClusterWithAuthentication() throws InterruptedException {
        ccm = CCMBridge.builder("test")
                .withCassandraConfiguration("authenticator", "PasswordAuthenticator")
                .notStarted()
                .build();
        ccm.start(1, "-Dcassandra.superuser_setup_delay_ms=0");

        // Even though we've override the default user setup delay, still wait
        // one second to make sure we don't race
        TimeUnit.SECONDS.sleep(1);
    }

    @AfterClass(groups = "short")
    public void shutdownCluster() {
        if (ccm != null)
            ccm.stop();
    }

    @Test(groups = "short")
    public void should_connect_with_credentials() throws InterruptedException {
        Cluster cluster = Cluster.builder().addContactPoint(CCMBridge.IP_PREFIX + '1')
                .withCredentials("cassandra", "cassandra")
                .build();
        try {
            cluster.connect();
        } catch (NoHostAvailableException e) {
            logger.error(e.getCustomMessage(1, true, true));
        } finally {
            cluster.close();
        }
    }

    @Test(groups = "short", expectedExceptions = AuthenticationException.class)
    public void should_fail_to_connect_with_wrong_credentials() throws InterruptedException {
        Cluster cluster = Cluster.builder().addContactPoint(CCMBridge.IP_PREFIX + '1')
                .withCredentials("bogus", "bogus")
                .build();
        try {
            cluster.connect();
        } catch (NoHostAvailableException e) {
            logger.error(e.getCustomMessage(1, true, true));
        } finally {
            cluster.close();
        }
    }

    @Test(groups = "short", expectedExceptions = AuthenticationException.class)
    public void should_fail_to_connect_without_credentials() throws InterruptedException {
        Cluster cluster = Cluster.builder().addContactPoint(CCMBridge.IP_PREFIX + '1')
                .build();
        try {
            cluster.connect();
        } catch (NoHostAvailableException e) {
            logger.error(e.getCustomMessage(1, true, true));
        } finally {
            cluster.close();
        }
    }
}
