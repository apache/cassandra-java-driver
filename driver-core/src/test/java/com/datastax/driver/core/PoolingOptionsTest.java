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

import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class PoolingOptionsTest extends CCMBridge.PerClassSingleNodeCluster {
    @Override
    protected Collection<String> getTableDefinitions() {
        return Collections.emptyList();
    }

    /**
     * <p>
     * Validates that if a custom executor is provided via {@link PoolingOptions#setInitializationExecutor} that it
     * is used to create and tear down connections.
     * </p>
     *
     * @test_category connection:connection_pool
     * @expected_result executor is used and successfully able to connect and tear down connections.
     * @jira_ticket JAVA-692
     * @since 2.0.10, 2.1.6
     */
    @Test(groups = "short")
    public void should_be_able_to_use_custom_initialization_executor() {
        ThreadPoolExecutor executor = spy(new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>()));

        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setInitializationExecutor(executor);

        Cluster cluster = Cluster.builder()
                .addContactPointsWithPorts(Collections.singletonList(hostAddress))
                .withPoolingOptions(poolingOptions).build();
        try {
            cluster.init();
            // Ensure executor used.
            verify(executor, atLeastOnce()).execute(any(Runnable.class));

            // Reset invocation count.
            reset();

            Session session = cluster.connect();

            // Ensure executor used again to establish core connections.
            verify(executor, atLeastOnce()).execute(any(Runnable.class));

            // Expect core connections + control connection.
            assertThat(cluster.getMetrics().getOpenConnections().getValue()).isEqualTo(
                    TestUtils.numberOfLocalCoreConnections(cluster) + 1);

            reset();

            session.close();

            // Executor should have been used to close connections associated with the session.
            verify(executor, atLeastOnce()).execute(any(Runnable.class));

            // Only the control connection should remain.
            assertThat(cluster.getMetrics().getOpenConnections().getValue()).isEqualTo(1);
        } finally {
            cluster.close();
            executor.shutdown();
        }
    }
}
