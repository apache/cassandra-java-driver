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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.*;

/**
 * A set of hooks that allow clients to customize the driver's internal executors.
 * <p/>
 * The methods in this class are invoked when the cluster initializes. To customize the behavior, extend the class and
 * override the appropriate methods.
 * <p/>
 * This is mainly intended to allow customization and instrumentation of driver threads. Each method must return a
 * newly-allocated executor; don't use a shared executor, as this could introduce unintended consequences like deadlocks
 * (we're working to simplify the driver's architecture and reduce the number of executors in a future release). The
 * default implementations use unbounded queues, which is appropriate when the driver is properly configured; the only
 * reason you would want to use bounded queues is to limit memory consumption in case of a bug or bad configuration. In
 * that case, make sure to use a {@link RejectedExecutionHandler} that throws, such as
 * {@link java.util.concurrent.ThreadPoolExecutor.AbortPolicy}; a blocking handler could introduce deadlocks.
 * <p/>
 * Netty uses a separate pool for I/O operations, that can be configured via {@link NettyOptions}.
 */
public class ThreadingOptions {
    // Kept for backward compatibility, but this should be customized via this class now
    private static final int NON_BLOCKING_EXECUTOR_SIZE = SystemProperties.getInt(
            "com.datastax.driver.NON_BLOCKING_EXECUTOR_SIZE", Runtime.getRuntime().availableProcessors());
    private static final int DEFAULT_THREAD_KEEP_ALIVE_SECONDS = 30;

    /**
     * Builds a thread factory for the threads created by a given executor.
     * <p/>
     * This is used by the default implementations in this class, and also internally to create the Netty I/O pool.
     *
     * @param clusterName  the name of the cluster, as specified by
     *                     {@link com.datastax.driver.core.Cluster.Builder#withClusterName(String)}.
     * @param executorName a name that identifies the executor.
     * @return the thread factory.
     */
    public ThreadFactory createThreadFactory(String clusterName, String executorName) {
        return new ThreadFactoryBuilder()
                .setNameFormat(clusterName + "-" + executorName + "-%d")
                // Back with Netty's thread factory in order to create FastThreadLocalThread instances. This allows
                // an optimization around ThreadLocals (we could use DefaultThreadFactory directly but it creates
                // slightly different thread names, so keep we keep a ThreadFactoryBuilder wrapper for backward
                // compatibility).
                .setThreadFactory(new DefaultThreadFactory("ignored name"))
                .build();
    }

    /**
     * Builds the main internal executor, used for tasks such as scheduling speculative executions, triggering
     * registered {@link SchemaChangeListener}s, reacting to node state changes, and metadata updates.
     * <p/>
     * The default implementation sets the pool size to the number of available cores.
     *
     * @param clusterName the name of the cluster, as specified by
     *                    {@link com.datastax.driver.core.Cluster.Builder#withClusterName(String)}.
     * @return the executor.
     */
    public ExecutorService createExecutor(String clusterName) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                NON_BLOCKING_EXECUTOR_SIZE, NON_BLOCKING_EXECUTOR_SIZE,
                DEFAULT_THREAD_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                createThreadFactory(clusterName, "worker"));
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /**
     * Builds the executor used to block on new connections before they are added to a pool.
     * <p/>
     * The default implementation uses 2 threads.
     *
     * @param clusterName the name of the cluster, as specified by
     *                    {@link com.datastax.driver.core.Cluster.Builder#withClusterName(String)}.
     * @return the executor.
     */
    public ExecutorService createBlockingExecutor(String clusterName) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2, 2,
                DEFAULT_THREAD_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                createThreadFactory(clusterName, "blocking-task-worker"));
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /**
     * Builds the executor when reconnection attempts will be scheduled.
     * <p/>
     * The default implementation uses 2 threads.
     *
     * @param clusterName the name of the cluster, as specified by
     *                    {@link com.datastax.driver.core.Cluster.Builder#withClusterName(String)}.
     * @return the executor.
     */
    public ScheduledExecutorService createReconnectionExecutor(String clusterName) {
        return new ScheduledThreadPoolExecutor(2, createThreadFactory(clusterName, "reconnection"));
    }

    /**
     * Builds the executor to handle host state notifications from Cassandra.
     * <p/>
     * <b>This executor must have exactly one thread</b> so that notifications are processed in order.
     *
     * @param clusterName the name of the cluster, as specified by
     *                    {@link com.datastax.driver.core.Cluster.Builder#withClusterName(String)}.
     * @return the executor.
     */
    public ScheduledExecutorService createScheduledTasksExecutor(String clusterName) {
        return new ScheduledThreadPoolExecutor(1, createThreadFactory(clusterName, "scheduled-task-worker"));
    }

    /**
     * Builds the executor for an internal maintenance task used to clean up closed connections.
     * <p/>
     * A single scheduled task runs on this executor, so there is no reason to use more than one thread.
     *
     * @param clusterName the name of the cluster, as specified by
     *                    {@link com.datastax.driver.core.Cluster.Builder#withClusterName(String)}.
     * @return the executor.
     */
    public ScheduledExecutorService createReaperExecutor(String clusterName) {
        return new ScheduledThreadPoolExecutor(1, createThreadFactory(clusterName, "connection-reaper"));
    }
}
