/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.util.concurrent.Uninterruptibles;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@CassandraVersion("2.0.0")
public class TracingTest extends CCMTestsSupport {

    private static final String KEY = "tracing_test";

    @Override
    public Cluster.Builder createClusterBuilder() {
        return Cluster.builder().withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
    }

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE test (k text, v int, PRIMARY KEY (k, v))");
        for (int i = 0; i < 100; i++) {
            execute(String.format("INSERT INTO test (k, v) VALUES ('%s', %d)", KEY, i));
        }
    }

    /**
     * Validates that for each page the {@link ExecutionInfo} will have a different tracing ID.
     *
     * @test_category tracing
     * @expected_result {@link ResultSet} where all the {@link ExecutionInfo} will contains a different tracing ID and
     * that the events can be retrieved for the last query.
     */
    @Test(groups = "short")
    public void should_have_a_different_tracingId_for_each_page() {
        SimpleStatement st = new SimpleStatement(String.format("SELECT v FROM test WHERE k='%s'", KEY));
        ResultSet result = session().execute(st.setFetchSize(40).enableTracing());
        result.all();
        // sleep 10 seconds to make sure the trace will be complete
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        List<ExecutionInfo> executions = result.getAllExecutionInfo();

        UUID previousTraceId = null;
        for (ExecutionInfo executionInfo : executions) {
            QueryTrace queryTrace = executionInfo.getQueryTrace();
            assertThat(queryTrace).isNotNull();
            assertThat(queryTrace.getTraceId()).isNotEqualTo(previousTraceId);
            previousTraceId = queryTrace.getTraceId();
        }

        assertThat(result.getExecutionInfo().getQueryTrace().getEvents()).isNotNull()
                .isNotEmpty();
    }

    /**
     * Validates that if a query gets retried, the second internal query will still have tracing enabled.
     * <p/>
     * To force a retry, we use the downgrading policy with an impossible CL.
     *
     * @test_category tracing
     * @jira_ticket JAVA-815
     * @expected_result {@link ResultSet} where {@link ExecutionInfo} contains trace information after a retry.
     */
    @Test(groups = "short")
    public void should_preserve_tracing_status_across_retries() {
        SimpleStatement st = new SimpleStatement(String.format("SELECT v FROM test WHERE k='%s'", KEY));
        st.setConsistencyLevel(ConsistencyLevel.THREE).enableTracing();

        ResultSet result = session().execute(st);
        // sleep 10 seconds to make sure the trace will be complete
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

        assertThat(result.getExecutionInfo().getQueryTrace()).isNotNull();
    }
}

