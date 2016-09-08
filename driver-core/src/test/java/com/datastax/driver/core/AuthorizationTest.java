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

import com.datastax.driver.core.exceptions.UnauthorizedException;
import com.google.common.util.concurrent.Uninterruptibles;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;

/**
 * Tests for user authorization.
 */
@CreateCCM(PER_METHOD)
@CCMConfig(
        config = {
                "authenticator:PasswordAuthenticator",
                "authorizer:CassandraAuthorizer"
        },
        jvmArgs = "-Dcassandra.superuser_setup_delay_ms=0",
        createCluster = false)
public class AuthorizationTest extends CCMTestsSupport {

    @BeforeMethod(groups = "short")
    public void sleepIf12() {
        // For C* 1.2, sleep before attempting to connect as there is a small delay between
        // user being created.
        if (ccm().getVersion().getMajor() < 2) {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
    }

    @Test(groups = "short", expectedExceptions = UnauthorizedException.class)
    public void should_fail_to_access_unauthorized_resource() throws InterruptedException {
        Cluster cluster1 = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withAuthProvider(new PlainTextAuthProvider("cassandra", "cassandra"))
                .build());
        Session session1 = cluster1.connect();
        session1.execute("CREATE USER johndoe WITH PASSWORD '1234'");
        String keyspace = TestUtils.generateIdentifier("ks_");
        session1.execute(String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1));
        session1.execute("USE " + keyspace);
        session1.execute("CREATE TABLE t1 (k int PRIMARY KEY, v int)");
        session1.execute("CREATE TABLE t2 (k int PRIMARY KEY, v int)");
        session1.execute(String.format("GRANT SELECT ON %s.t1 TO johndoe;", keyspace));
        Cluster cluster2 = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withAuthProvider(new PlainTextAuthProvider("johndoe", "1234"))
                .build());
        Session session2 = cluster2.connect(keyspace);
        // should pass
        session2.execute("SELECT * FROM t1 WHERE k = 1");
        // should throw UnauthorizedException
        session2.execute("SELECT * FROM t2 WHERE k = 1");
    }


}
