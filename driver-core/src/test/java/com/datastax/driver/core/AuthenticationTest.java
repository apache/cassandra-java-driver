/*
 *      Copyright (C) 2012 DataStax Inc.
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
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * Tests for authenticated cluster access
 */
public class AuthenticationTest {

    private static final Logger logger = LoggerFactory.getLogger(AuthenticationTest.class);

    private CCMBridge cassandraCluster;

    /**
     * creates a cluster and turns on password authentication before starting it.
     */
    @BeforeClass (groups = "short")
    public void setupClusterWithAuthentication() throws InterruptedException {
        cassandraCluster = CCMBridge.create("test");
        cassandraCluster.populate(1);
        cassandraCluster.updateConfig("authenticator", PasswordAuthenticator.class.getName());
        cassandraCluster.start(1);

        // Pre-1.2.4, we cannot override the pause before creating the default user,
        // so we need to sleep for 10 seconds while we wait for it
        TimeUnit.SECONDS.sleep(10);
    }

    @AfterClass (groups = "short")
    public void shutdownCluster() {
        if (null != cassandraCluster)
            cassandraCluster.stop();
    }

    @Test(groups = "short")
    public void testAuthenticatedConnection() throws InterruptedException {
        try {
            Cluster.builder().addContactPoint(CCMBridge.IP_PREFIX + "1")
                                                .withCredentials("cassandra", "cassandra")
                                                .build()
                                                .connect();
        } catch (NoHostAvailableException e) {

            for (Map.Entry<InetAddress, String> entry : e.getErrors().entrySet())
                logger.info("Error connecting to " + entry.getKey() + ": " + entry.getValue());
            throw new RuntimeException(e);
        }
    }

    @Test(groups = "short", expectedExceptions = AuthenticationException.class)
    public void testConnectionAttemptWithIncorrectCredentialsIsRefused() throws InterruptedException {
        try {
            Cluster.builder().addContactPoint(CCMBridge.IP_PREFIX + "1")
                   .withCredentials("bogus", "bogus")
                   .build()
                   .connect();
        } catch (NoHostAvailableException e) {

            for (Map.Entry<InetAddress, String> entry : e.getErrors().entrySet())
                logger.info("Error connecting to " + entry.getKey() + ": " + entry.getValue());
            throw new RuntimeException(e);
        }
    }

    @Test(groups = "short", expectedExceptions = AuthenticationException.class)
    public void testConnectionAttemptWithoutCredentialsIsRefused() throws InterruptedException {
        try {
            Cluster.builder().addContactPoint(CCMBridge.IP_PREFIX + "1")
                              .build()
                              .connect();
        } catch (NoHostAvailableException e) {

            for (Map.Entry<InetAddress, String> entry : e.getErrors().entrySet())
                logger.info("Error connecting to " + entry.getKey() + ": " + entry.getValue());
            throw new RuntimeException(e);
        }
    }
}
