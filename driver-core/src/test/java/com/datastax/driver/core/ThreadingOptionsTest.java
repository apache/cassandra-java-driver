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

import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.assertj.core.api.iterable.Extractor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.scassandra.http.client.ClosedConnectionReport.CloseType.CLOSE;

public class ThreadingOptionsTest extends ScassandraTestBase {

    private String customPrefix = "custom";

    private ThreadingOptions threadingOptions = new ThreadingOptions() {

        @Override
        public ThreadFactory createThreadFactory(String clusterName, String executorName) {
            return new ThreadFactoryBuilder()
                    .setNameFormat(clusterName + "-" + customPrefix + "-" + executorName + "-%d")
                    // Back with Netty's thread factory in order to create FastThreadLocalThread instances. This allows
                    // an optimization around ThreadLocals (we could use DefaultThreadFactory directly but it creates
                    // slightly different thread names, so keep we keep a ThreadFactoryBuilder wrapper for backward
                    // compatibility).
                    .setThreadFactory(new DefaultThreadFactory("ignored name"))
                    .setDaemon(true)
                    .build();
        }

        @Override
        public ExecutorService createExecutor(String clusterName) {
            return new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    createThreadFactory(clusterName, "myExecutor")
            );
        }

        @Override
        public ExecutorService createBlockingExecutor(String clusterName) {
            return new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    createThreadFactory(clusterName, "myBlockingExecutor")
            );
        }

        @Override
        public ScheduledExecutorService createReconnectionExecutor(String clusterName) {
            return new ScheduledThreadPoolExecutor(1, createThreadFactory(clusterName, "myReconnection"));
        }

        @Override
        public ScheduledExecutorService createScheduledTasksExecutor(String clusterName) {
            return new ScheduledThreadPoolExecutor(1, createThreadFactory(clusterName, "myScheduled-task-worker"));
        }

        @Override
        public ScheduledExecutorService createReaperExecutor(String clusterName) {
            return new ScheduledThreadPoolExecutor(1, createThreadFactory(clusterName, "myConnection-reaper"));
        }
    };

    /**
     * Validates that when using a provided {@link ThreadingOptions} that its methods are used for creating
     * executors and that its {@link ThreadingOptions#createThreadFactory(String, String)} is used for initializing
     * netty resources.
     *
     * @test_category configuration
     */
    @Test(groups = "short")
    public void should_use_provided_threading_options() {
        ThreadingOptions spy = Mockito.spy(threadingOptions);
        Cluster cluster = createClusterBuilder().withPoolingOptions(new PoolingOptions()
                .setConnectionsPerHost(HostDistance.LOCAL, 1, 1))
                .withReconnectionPolicy(new ConstantReconnectionPolicy(100))
                .withThreadingOptions(spy).build();
        try {
            String clusterName = cluster.getClusterName();
            cluster.init();

            // Ensure each method was invoked appropriately:
            // 1) 1 time for each create*Executor.
            // 2) createThreadFactory for netty executor group and timeouter.
            verify(spy).createExecutor(clusterName);
            verify(spy).createBlockingExecutor(clusterName);
            verify(spy).createReconnectionExecutor(clusterName);
            verify(spy).createScheduledTasksExecutor(clusterName);
            verify(spy).createReaperExecutor(clusterName);
            verify(spy).createThreadFactory(clusterName, "nio-worker");
            verify(spy).createThreadFactory(clusterName, "timeouter");

            cluster.connect();

            // Close all connections bringing the host down, this should cause some activity on
            // executor and reconnection executor.
            currentClient.disableListener();
            currentClient.closeConnections(CLOSE);
            TestUtils.waitForDown(TestUtils.IP_PREFIX + "1", cluster);
            currentClient.enableListener();
            TestUtils.waitForUp(TestUtils.IP_PREFIX + "1", cluster);

            Set<Thread> threads = Thread.getAllStackTraces().keySet();
            for(Thread thread : threads) {
                // all threads should use the custom factory and thus be marked daemon
                if(thread.getName().startsWith(clusterName + "-" + customPrefix)) {
                    // all created threads should be daemon this should indicate that our custom thread factory was
                    // used.
                    assertThat(thread.isDaemon()).isTrue();
                }
            }

            final Pattern threadNamePattern = Pattern.compile(clusterName + "-" + customPrefix + "-(.*)-0");

            // Custom executor threads should be present.
            // NOTE: we don't validate blocking executor since it is hard to deterministically cause it to be used.
            assertThat(threads).extracting(new Extractor<Thread, String>() {
                @Override
                public String extract(Thread thread) {
                    Matcher matcher = threadNamePattern.matcher(thread.getName());
                    if(matcher.matches()) {
                        return matcher.group(1);
                    } else {
                        return thread.getName();
                    }
                }
            }).contains(
                    "nio-worker",
                    "timeouter",
                    "myExecutor",
                    "myReconnection",
                    "myScheduled-task-worker",
                    "myConnection-reaper"
            );
        } finally {
            cluster.close();
        }
    }
}
