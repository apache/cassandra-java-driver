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
import com.google.common.util.concurrent.Uninterruptibles;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;

/**
 * Tests for authenticated cluster access
 */
@CreateCCM(PER_METHOD)
@CCMConfig(
        config = "authenticator:PasswordAuthenticator",
        jvmArgs = "-Dcassandra.superuser_setup_delay_ms=0",
        createCluster = false)
public class AuthenticationTest extends CCMTestsSupport {


    @BeforeMethod(groups = "short")
    public void sleepIf12() {
        // For C* 1.2, sleep before attempting to connect as there is a small delay between
        // user being created.
        if (ccm.getVersion().getMajor() < 2) {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
    }

    @Test(groups = "short")
    public void should_connect_with_credentials() throws InterruptedException {
        Cluster cluster = register(Cluster.builder()
                .addContactPointsWithPorts(getInitialContactPoints())
                .withCredentials("cassandra", "cassandra")
                .build());
        cluster.connect();
    }

    @Test(groups = "short", expectedExceptions = AuthenticationException.class)
    public void should_fail_to_connect_with_wrong_credentials() throws InterruptedException {
        Cluster cluster = register(Cluster.builder()
                .addContactPointsWithPorts(getInitialContactPoints())
                .withCredentials("bogus", "bogus")
                .build());
        cluster.connect();
    }

    @Test(groups = "short", expectedExceptions = AuthenticationException.class)
    public void should_fail_to_connect_without_credentials() throws InterruptedException {
        Cluster cluster = register(Cluster.builder()
                .addContactPointsWithPorts(getInitialContactPoints())
                .build());
        cluster.connect();
    }

}
