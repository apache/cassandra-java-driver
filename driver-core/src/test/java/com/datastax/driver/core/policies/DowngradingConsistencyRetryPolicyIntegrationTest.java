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
import com.google.common.base.Objects;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;
import org.testng.annotations.Test;

import static com.datastax.driver.core.ConsistencyLevel.*;
import static com.datastax.driver.core.TestUtils.ipOfNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

/**
 * Note: we can't extend {@link AbstractRetryPolicyIntegrationTest} here, because SCassandra doesn't allow custom values for
 * receivedResponses in primed responses.
 * If that becomes possible in the future, we could refactor this test.
 */
@CCMConfig(
        dirtiesContext = true,
        createCluster = false,
        numberOfNodes = 3,
        config = {
                // tests fail often with write or read timeouts
                "hinted_handoff_enabled:true",
                "phi_convict_threshold:5",
                "read_request_timeout_in_ms:100000",
                "write_request_timeout_in_ms:100000"
        }
)
public class DowngradingConsistencyRetryPolicyIntegrationTest extends CCMTestsSupport {

    @Test(groups = "long")
    public void should_downgrade_if_not_enough_replicas_for_requested_CL() {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(120000))
                .withRetryPolicy(Mockito.spy(DowngradingConsistencyRetryPolicy.INSTANCE))
                .build());
        Session session = cluster.connect();

        String ks = TestUtils.generateIdentifier("ks_");
        session.execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}", ks));
        useKeyspace(session, ks);
        session.execute("CREATE TABLE foo(k int primary key)");
        session.execute("INSERT INTO foo(k) VALUES (0)");

        // All replicas up: should achieve all levels without downgrading
        checkAchievedConsistency(ALL, ALL, session);
        checkAchievedConsistency(QUORUM, QUORUM, session);
        checkAchievedConsistency(ONE, ONE, session);

        stopAndWait(1, cluster);
        // Two replicas remaining: should downgrade to 2 when CL > 2
        checkAchievedConsistency(ALL, TWO, session);
        checkAchievedConsistency(QUORUM, QUORUM, session); // since RF = 3, quorum is still achievable with two nodes
        checkAchievedConsistency(TWO, TWO, session);
        checkAchievedConsistency(ONE, ONE, session);

        stopAndWait(2, cluster);
        // One replica remaining: should downgrade to 1 when CL > 1
        checkAchievedConsistency(ALL, ONE, session);
        checkAchievedConsistency(QUORUM, ONE, session);
        checkAchievedConsistency(TWO, ONE, session);
        checkAchievedConsistency(ONE, ONE, session);

    }

    private void stopAndWait(int node, Cluster cluster) {
        ccm().stop(node);
        ccm().waitForDown(node);// this uses port ping
        TestUtils.waitForDown(ipOfNode(node), cluster); // this uses UP/DOWN events
    }

    private void checkAchievedConsistency(ConsistencyLevel requested, ConsistencyLevel expected, Session session) {
        RetryPolicy retryPolicy = session.getCluster().getConfiguration().getPolicies().getRetryPolicy();
        Mockito.reset(retryPolicy);

        Statement s = new SimpleStatement("SELECT * FROM foo WHERE k = 0")
                .setConsistencyLevel(requested);

        ResultSet rs = session.execute(s);

        ConsistencyLevel achieved = rs.getExecutionInfo().getAchievedConsistencyLevel();
        // ExecutionInfo returns null when the requested level was met.
        ConsistencyLevel actual = Objects.firstNonNull(achieved, requested);

        assertThat(actual).isEqualTo(expected);

        // If the level was downgraded the policy should have been invoked
        VerificationMode expectedCallsToPolicy = (expected == requested) ? never() : times(1);
        Mockito.verify(retryPolicy, expectedCallsToPolicy).onUnavailable(
                any(Statement.class), any(ConsistencyLevel.class), anyInt(), anyInt(), anyInt());
    }
}
