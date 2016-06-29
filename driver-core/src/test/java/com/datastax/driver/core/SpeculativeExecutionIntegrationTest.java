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

import com.datastax.driver.core.policies.ConstantSpeculativeExecutionPolicy;
import com.datastax.driver.core.utils.CassandraVersion;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.times;

/**
 * Test that needs a real CCM cluster (as opposed to SCassandra for other specex tests), because
 * it uses a protocol v3 feature.
 */
public class SpeculativeExecutionIntegrationTest extends CCMTestsSupport {

    TimestampGenerator timestampGenerator;

    @Override
    public void onTestContextInitialized() {
        execute("create table foo(k int primary key, v int)");
    }

    @Override
    public Cluster.Builder createClusterBuilder() {
        timestampGenerator = Mockito.spy(ServerSideTimestampGenerator.INSTANCE);
        return Cluster.builder()
                .withTimestampGenerator(timestampGenerator)
                // Set an artificially low timeout to force speculative execution
                .withSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(1, 2));
    }

    /**
     * Validates that if a query gets speculatively re-executed, the second execution uses the same default timestamp.
     *
     * @test_category tracing
     * @jira_ticket JAVA-724
     * @expected_result timestamp generator invoked only once for a query that caused two executions.
     */
    @Test(groups = "short")
    @CassandraVersion(major = 2.1)
    public void should_use_same_default_timestamp_for_all_executions() {
        Metrics.Errors errors = cluster().getMetrics().getErrorMetrics();

        // The check is attempted up to 10 times to account for the small possibility that a
        // scheduled execution is not needed/exercised.  Even though the policy is set up
        // to schedule an execution after 1ms, the timeout might not fire before the response is received.
        int tryCount = 0;
        int maxTries = 10;
        while (tryCount++ < maxTries) {
            Mockito.reset(timestampGenerator);
            long execStartCount = errors.getSpeculativeExecutions().getCount();

            BatchStatement batch = new BatchStatement();
            for (int k = 0; k < 1000; k++) {
                batch.add(new SimpleStatement("insert into foo(k,v) values (?,1)", k).setIdempotent(true));
            }
            batch.setIdempotent(true);
            session().execute(batch);

            if (errors.getSpeculativeExecutions().getCount() == execStartCount + 1) {
                Mockito.verify(timestampGenerator, times(1)).next();
                break;
            }
        }

        if (tryCount == maxTries) {
            fail("Observed no speculative executions in 10 attempts");
        }
    }
}
