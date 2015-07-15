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

import java.util.Collection;

import com.google.common.collect.Lists;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;

import com.datastax.driver.core.policies.ConstantSpeculativeExecutionPolicy;
import com.datastax.driver.core.utils.CassandraVersion;

/**
 * Test that needs a real CCM cluster (as opposed to SCassandra for other specex tests), because
 * it uses a protocol v3 feature.
 */
public class SpeculativeExecutionIntegrationTest extends CCMBridge.PerClassSingleNodeCluster {

    TimestampGenerator timestampGenerator;

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList("create table foo(k int primary key, v int)");
    }

    @Override
    protected Cluster.Builder configure(Cluster.Builder builder) {
        timestampGenerator = Mockito.spy(ServerSideTimestampGenerator.INSTANCE);
        return builder
            .withTimestampGenerator(timestampGenerator)
            .withQueryOptions(new QueryOptions().setDefaultIdempotence(true))
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
        Metrics.Errors errors = cluster.getMetrics().getErrorMetrics();

        Mockito.reset(timestampGenerator);
        long execStartCount = errors.getSpeculativeExecutions().getCount();

        BatchStatement batch = new BatchStatement();
        for (int k = 0; k < 1000; k++) {
            batch.add(new SimpleStatement("insert into foo(k,v) values (?,1)", k));
        }
        session.execute(batch);

        assertThat(errors.getSpeculativeExecutions().getCount()).isEqualTo(execStartCount + 1);
        Mockito.verify(timestampGenerator, times(1)).next();
    }
}
