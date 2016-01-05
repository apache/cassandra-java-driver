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
import org.testng.annotations.Test;

import static com.datastax.driver.core.CCMTestMode.TestMode.PER_METHOD;

/**
 * Tests for authenticated cluster access
 */
@CCMTestMode(PER_METHOD)
@CCMConfig(createSession = false)
@SuppressWarnings("unused")
public class AuthenticationTest extends CCMTestsSupport {

    @CCMConfig(clusterProvider = "rightCredentials")
    @Test(groups = "short")
    public void should_connect_with_credentials() throws InterruptedException {
        cluster.connect();
    }

    @CCMConfig(clusterProvider = "wrongCredentials")
    @Test(groups = "short", expectedExceptions = AuthenticationException.class)
    public void should_fail_to_connect_with_wrong_credentials() throws InterruptedException {
        cluster.connect();
    }

    @Test(groups = "short", expectedExceptions = AuthenticationException.class)
    public void should_fail_to_connect_without_credentials() throws InterruptedException {
        cluster.connect();
    }

    @Override
    public CCMBridge.Builder createCCMBridgeBuilder() {
        return super.createCCMBridgeBuilder()
                .withCassandraConfiguration("authenticator", "PasswordAuthenticator")
                .withJvmArgs("-Dcassandra.superuser_setup_delay_ms=0");
    }

    public Cluster.Builder rightCredentials() {
        return Cluster.builder()
                .withCredentials("cassandra", "cassandra");
    }

    public Cluster.Builder wrongCredentials() {
        return Cluster.builder()
                .withCredentials("bogus", "bogus");
    }

}
