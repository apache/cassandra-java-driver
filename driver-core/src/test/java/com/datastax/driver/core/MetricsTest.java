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

import com.datastax.driver.core.Metrics.Errors;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.Collection;

import static org.testng.Assert.assertEquals;

public class MetricsTest extends CCMBridge.PerClassSingleNodeCluster {
    private volatile RetryDecision retryDecision;

    @Override
    protected Cluster.Builder configure(Cluster.Builder builder) {
        return builder.withRetryPolicy(new RetryPolicy() {
            @Override
            public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
                return retryDecision;
            }

            @Override
            public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
                return retryDecision;
            }

            @Override
            public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
                return retryDecision;
            }
        });
    }

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList("CREATE TABLE test (k int primary key, v int)",
                "INSERT INTO test (k, v) VALUES (1, 1)");
    }

    @Test(groups = "short")
    public void retriesTest() {
        retryDecision = RetryDecision.retry(ConsistencyLevel.ONE);

        // We only have one node, this will throw an unavailable exception
        Statement statement = new SimpleStatement("SELECT v FROM test WHERE k = 1").setConsistencyLevel(ConsistencyLevel.TWO);
        session.execute(statement);

        Errors errors = cluster.getMetrics().getErrorMetrics();
        assertEquals(errors.getUnavailables().getCount(), 1);
        assertEquals(errors.getRetries().getCount(), 1);
        assertEquals(errors.getRetriesOnUnavailable().getCount(), 1);

        retryDecision = RetryDecision.ignore();
        session.execute(statement);

        assertEquals(errors.getUnavailables().getCount(), 2);
        assertEquals(errors.getIgnores().getCount(), 1);
        assertEquals(errors.getIgnoresOnUnavailable().getCount(), 1);
    }
}
