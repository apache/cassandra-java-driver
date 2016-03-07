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
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.ConsistencyLevel.ANY;
import static com.datastax.driver.core.ConsistencyLevel.SERIAL;
import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests for retry policies in combination with lightweight transactions.
 * <p/>
 * Note: we can't extend {@link AbstractRetryPolicyIntegrationTest} here, because we are
 * testing LWT statements and Scassandra doesn't support priming consistency levels yet.
 *
 * @jira_ticket JAVA-764
 */
@CCMConfig(
        numberOfNodes = 3,
        dirtiesContext = true,
        createCluster = false
)
@CreateCCM(PER_METHOD)
@CassandraVersion(major = 2.0)
public class CASRetryPolicyIntegrationTest extends CCMTestsSupport {

    @Test(groups = "long")
    public void should_rethrow_on_unavailable_with_default_policy_if_CAS() {
        testLWT(spy(DefaultRetryPolicy.INSTANCE));
    }

    @Test(groups = "long")
    public void should_rethrow_on_unavailable_with_downgrading_policy_if_CAS() {
        testLWT(spy(DowngradingConsistencyRetryPolicy.INSTANCE));
    }

    private void testLWT(RetryPolicy retryPolicy) {

        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(120000))
                .withRetryPolicy(retryPolicy)
                .build());

        Session session = cluster.connect();

        String ks = TestUtils.generateIdentifier("ks_");
        session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}", ks));
        useKeyspace(session, ks);
        session.execute("CREATE TABLE IF NOT EXISTS foo (k int primary key, c int)");

        session.execute("INSERT INTO foo(k, c) VALUES (0, 0)");

        ccm().stop(1);
        ccm().waitForDown(1);
        TestUtils.waitForDown(ccm().addressOfNode(1).getHostName(), cluster);

        ccm().stop(2);
        ccm().waitForDown(2);
        TestUtils.waitForDown(ccm().addressOfNode(2).getHostName(), cluster);

        Statement s = new SimpleStatement("UPDATE foo SET c = 1 WHERE k = 0 IF c = 0")
                // the following will cause the paxos phase to fail
                // given the number of available replicas (1)
                .setConsistencyLevel(ANY)
                .setSerialConsistencyLevel(SERIAL);

        try {
            session.execute(s);
            // Expect a NHAE since a retry should have been attempted on unavailable error with CL SERIAL.
            fail("Expected NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            Throwable error = e.getErrors().values().iterator().next();
            assertThat(error).isInstanceOf(UnavailableException.class);
            UnavailableException unavailable = (UnavailableException) error;
            assertThat(unavailable.getConsistencyLevel()).isEqualTo(SERIAL);
            assertThat(unavailable.getRequiredReplicas()).isEqualTo(2);
            assertThat(unavailable.getAliveReplicas()).isEqualTo(1);
        }

        verify(retryPolicy, times(1)).onUnavailable(
                eq(s), eq(SERIAL), eq(2), eq(1), eq(0));
    }

}
