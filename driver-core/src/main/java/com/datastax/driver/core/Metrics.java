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

import com.codahale.metrics.*;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;

import java.util.HashSet;
import java.util.Set;

/**
 * Metrics exposed by the driver.
 * <p/>
 * The metrics exposed by this class use the <a href="http://metrics.codahale.com/">Metrics</a>
 * library and you should refer its <a href="http://metrics.codahale.com/manual/">documentation</a>
 * for details on how to handle the exposed metric objects.
 * <p/>
 * By default, metrics are exposed through JMX, which is very useful for
 * development and browsing, but for production environments you may want to
 * have a look at the <a href="http://metrics.codahale.com/manual/core/#reporters">reporters</a>
 * provided by the Metrics library which could be more efficient/adapted.
 */
public class Metrics {

    private final Cluster.Manager manager;
    private final MetricRegistry registry = new MetricRegistry();
    private final JmxReporter jmxReporter;
    private final Errors errors = new Errors();

    private final Timer requests = registry.timer("requests");

    private final Gauge<Integer> knownHosts = registry.register("known-hosts", new Gauge<Integer>() {
        @Override
        public Integer getValue() {
            return manager.metadata.allHosts().size();
        }
    });
    private final Gauge<Integer> connectedTo = registry.register("connected-to", new Gauge<Integer>() {
        @Override
        public Integer getValue() {
            Set<Host> s = new HashSet<Host>();
            for (SessionManager session : manager.sessions)
                s.addAll(session.pools.keySet());
            return s.size();
        }
    });
    private final Gauge<Integer> openConnections = registry.register("open-connections", new Gauge<Integer>() {
        @Override
        public Integer getValue() {
            int value = manager.controlConnection.isOpen() ? 1 : 0;
            for (SessionManager session : manager.sessions)
                for (HostConnectionPool pool : session.pools.values())
                    value += pool.opened();
            return value;
        }
    });
    private final Gauge<Integer> trashedConnections = registry.register("trashed-connections", new Gauge<Integer>() {
        @Override
        public Integer getValue() {
            int value = 0;
            for (SessionManager session : manager.sessions)
                for (HostConnectionPool pool : session.pools.values())
                    value += pool.trashed();
            return value;
        }
    });

    private final Gauge<Integer> executorQueueDepth = registry.register("executor-queue-depth", new Gauge<Integer>() {
        @Override
        public Integer getValue() {
            return manager.executorQueue.size();
        }
    });

    private final Gauge<Integer> blockingExecutorQueueDepth = registry.register("blocking-executor-queue-depth", new Gauge<Integer>() {
        @Override
        public Integer getValue() {
            return manager.blockingExecutorQueue.size();
        }
    });

    private final Gauge<Integer> reconnectionSchedulerQueueSize = registry.register("reconnection-scheduler-task-count", new Gauge<Integer>() {
        @Override
        public Integer getValue() {
            return manager.reconnectionExecutor.getQueue().size();
        }
    });

    private final Gauge<Integer> taskSchedulerQueueSize = registry.register("task-scheduler-task-count", new Gauge<Integer>() {
        @Override
        public Integer getValue() {
            return manager.scheduledTasksExecutor.getQueue().size();
        }
    });

    Metrics(Cluster.Manager manager) {
        this.manager = manager;
        if (manager.configuration.getMetricsOptions().isJMXReportingEnabled()) {
            this.jmxReporter = JmxReporter.forRegistry(registry).inDomain(manager.clusterName + "-metrics").build();
            this.jmxReporter.start();
        } else {
            this.jmxReporter = null;
        }
    }

    /**
     * Returns the registry containing all metrics.
     * <p/>
     * The metrics registry allows you to easily use the reporters that ship
     * with <a href="http://metrics.codahale.com/manual/core/#reporters">Metrics</a>
     * or a custom written one.
     * <p/>
     * For instance, if {@code metrics} is {@code this} object, you could export the
     * metrics to csv files using:
     * <pre>
     *     com.codahale.metrics.CsvReporter.forRegistry(metrics.getRegistry()).build(new File("measurements/")).start(1, TimeUnit.SECONDS);
     * </pre>
     * <p/>
     * If you already have a {@code MetricRegistry} in your application and wish to
     * add the driver's metrics to it, the recommended approach is to use a listener:
     * <pre>
     *     // Your existing registry:
     *     final com.codahale.metrics.MetricRegistry myRegistry = ...
     *
     *     cluster.getMetrics().getRegistry().addListener(new com.codahale.metrics.MetricRegistryListener() {
     *         &#64;Override
     *         public void onGaugeAdded(String name, Gauge&lt;?&gt; gauge) {
     *             if (myRegistry.getNames().contains(name)) {
     *                 // name is already taken, maybe prefix with a namespace
     *                 ...
     *             } else {
     *                 myRegistry.register(name, gauge);
     *             }
     *         }
     *
     *         ... // Implement other methods in a similar fashion
     *     });
     * </pre>
     * Since reporting is handled by your registry, you'll probably also want to disable
     * JMX reporting with {@link Cluster.Builder#withoutJMXReporting()}.
     *
     * @return the registry containing all metrics.
     */
    public MetricRegistry getRegistry() {
        return registry;
    }

    /**
     * Returns metrics on the user requests performed on the Cluster.
     * <p/>
     * This metric exposes
     * <ul>
     * <li>the total number of requests.</li>
     * <li>the requests rate (in requests per seconds), including 1, 5 and 15 minute rates.</li>
     * <li>the mean, min and max latencies, as well as latency at a given percentile.</li>
     * </ul>
     *
     * @return a {@code Timer} metric object exposing the rate and latency for
     * user requests.
     */
    public Timer getRequestsTimer() {
        return requests;
    }

    /**
     * Returns an object grouping metrics related to the errors encountered.
     *
     * @return an object grouping metrics related to the errors encountered.
     */
    public Errors getErrorMetrics() {
        return errors;
    }

    /**
     * Returns the number of Cassandra hosts currently known by the driver (that is
     * whether they are currently considered up or down).
     *
     * @return the number of Cassandra hosts currently known by the driver.
     */
    public Gauge<Integer> getKnownHosts() {
        return knownHosts;
    }

    /**
     * Returns the number of Cassandra hosts the driver is currently connected to
     * (that is have at least one connection opened to).
     *
     * @return the number of Cassandra hosts the driver is currently connected to.
     */
    public Gauge<Integer> getConnectedToHosts() {
        return connectedTo;
    }

    /**
     * Returns the total number of currently opened connections to Cassandra hosts.
     *
     * @return The total number of currently opened connections to Cassandra hosts.
     */
    public Gauge<Integer> getOpenConnections() {
        return openConnections;
    }

    /**
     * Returns the total number of currently "trashed" connections to Cassandra hosts.
     * <p/>
     * When the load to a host decreases, the driver will reclaim some connections in order to save
     * resources. No requests are sent to these connections anymore, but they are kept open for an
     * additional amount of time ({@link PoolingOptions#getIdleTimeoutSeconds()}), in case the load
     * goes up again. This metric counts connections in that state.
     *
     * @return The total number of currently trashed connections to Cassandra hosts.
     */
    public Gauge<Integer> getTrashedConnections() {
        return trashedConnections;
    }

    /**
     * @return The number of queued up tasks in the non-blocking executor (Cassandra Java Driver workers).
     */
    public Gauge<Integer> getExecutorQueueDepth() {
        return executorQueueDepth;
    }

    /**
     * @return The number of queued up tasks in the blocking executor (Cassandra Java Driver blocking tasks worker).
     */
    public Gauge<Integer> getBlockingExecutorQueueDepth() {
        return blockingExecutorQueueDepth;
    }

    /**
     * @return The size of the work queue for the reconnection scheduler (Reconnection).  A queue size > 0 does not
     * necessarily indicate a backlog as some tasks may not have been scheduled to execute yet.
     */
    public Gauge<Integer> getReconnectionSchedulerQueueSize() {
        return reconnectionSchedulerQueueSize;
    }

    /**
     * @return The size of the work queue for the task scheduler (Scheduled Tasks).  A queue size > 0 does not
     * necessarily indicate a backlog as some tasks may not have been scheduled to execute yet.
     */
    public Gauge<Integer> getTaskSchedulerQueueSize() {
        return taskSchedulerQueueSize;
    }

    void shutdown() {
        if (jmxReporter != null)
            jmxReporter.stop();
    }

    /**
     * Metrics on errors encountered.
     */
    public class Errors {

        private final Counter connectionErrors = registry.counter("connection-errors");

        private final Counter writeTimeouts = registry.counter("write-timeouts");
        private final Counter readTimeouts = registry.counter("read-timeouts");
        private final Counter unavailables = registry.counter("unavailables");
        private final Counter clientTimeouts = registry.counter("client-timeouts");

        private final Counter otherErrors = registry.counter("other-errors");

        private final Counter retries = registry.counter("retries");
        private final Counter retriesOnWriteTimeout = registry.counter("retries-on-write-timeout");
        private final Counter retriesOnReadTimeout = registry.counter("retries-on-read-timeout");
        private final Counter retriesOnUnavailable = registry.counter("retries-on-unavailable");
        private final Counter retriesOnClientTimeout = registry.counter("retries-on-client-timeout");
        private final Counter retriesOnConnectionError = registry.counter("retries-on-connection-error");
        private final Counter retriesOnOtherErrors = registry.counter("retries-on-other-errors");

        private final Counter ignores = registry.counter("ignores");
        private final Counter ignoresOnWriteTimeout = registry.counter("ignores-on-write-timeout");
        private final Counter ignoresOnReadTimeout = registry.counter("ignores-on-read-timeout");
        private final Counter ignoresOnUnavailable = registry.counter("ignores-on-unavailable");
        private final Counter ignoresOnClientTimeout = registry.counter("ignores-on-client-timeout");
        private final Counter ignoresOnConnectionError = registry.counter("ignores-on-connection-error");
        private final Counter ignoresOnOtherErrors = registry.counter("ignores-on-other-errors");

        private final Counter speculativeExecutions = registry.counter("speculative-executions");

        /**
         * Returns the number of connection to Cassandra nodes errors.
         * <p/>
         * This represents the number of times that a request to a Cassandra node
         * has failed due to a connection problem. This thus also corresponds to
         * how often the driver had to pick a fallback host for a request.
         * <p/>
         * You can expect a few connection errors when a Cassandra node fails
         * (or is stopped) ,but if that number grows continuously you likely have
         * a problem.
         *
         * @return the number of connection to Cassandra nodes errors.
         */
        public Counter getConnectionErrors() {
            return connectionErrors;
        }

        /**
         * Returns the number of write requests that returned a timeout (independently
         * of the final decision taken by the {@link com.datastax.driver.core.policies.RetryPolicy}).
         *
         * @return the number of write timeout.
         */
        public Counter getWriteTimeouts() {
            return writeTimeouts;
        }

        /**
         * Returns the number of read requests that returned a timeout (independently
         * of the final decision taken by the {@link com.datastax.driver.core.policies.RetryPolicy}).
         *
         * @return the number of read timeout.
         */
        public Counter getReadTimeouts() {
            return readTimeouts;
        }

        /**
         * Returns the number of requests that returned an unavailable exception
         * (independently of the final decision taken by the
         * {@link com.datastax.driver.core.policies.RetryPolicy}).
         *
         * @return the number of unavailable exceptions.
         */
        public Counter getUnavailables() {
            return unavailables;
        }

        /**
         * Returns the number of requests that timed out before the driver
         * received a response.
         *
         * @return the number of client timeouts.
         */
        public Counter getClientTimeouts() {
            return clientTimeouts;
        }

        /**
         * Returns the number of requests that returned errors not accounted for by
         * another metric. This includes all types of invalid requests.
         *
         * @return the number of requests errors not accounted by another
         * metric.
         */
        public Counter getOthers() {
            return otherErrors;
        }

        /**
         * Returns the number of times a request was retried due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}.
         *
         * @return the number of times a request was retried due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}.
         */
        public Counter getRetries() {
            return retries;
        }

        /**
         * Returns the number of times a request was retried due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * read timed out.
         *
         * @return the number of times a request was retried due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * read timed out.
         */
        public Counter getRetriesOnReadTimeout() {
            return retriesOnReadTimeout;
        }

        /**
         * Returns the number of times a request was retried due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * write timed out.
         *
         * @return the number of times a request was retried due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * write timed out.
         */
        public Counter getRetriesOnWriteTimeout() {
            return retriesOnWriteTimeout;
        }

        /**
         * Returns the number of times a request was retried due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after an
         * unavailable exception.
         *
         * @return the number of times a request was retried due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after an
         * unavailable exception.
         */
        public Counter getRetriesOnUnavailable() {
            return retriesOnUnavailable;
        }

        /**
         * Returns the number of times a request was retried due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * client timeout.
         *
         * @return the number of times a request was retried due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * client timeout.
         */
        public Counter getRetriesOnClientTimeout() {
            return retriesOnClientTimeout;
        }

        /**
         * Returns the number of times a request was retried due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * connection error.
         *
         * @return the number of times a request was retried due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * connection error.
         */
        public Counter getRetriesOnConnectionError() {
            return retriesOnConnectionError;
        }

        /**
         * Returns the number of times a request was retried due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after an
         * unexpected error.
         *
         * @return the number of times a request was retried due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after an
         * unexpected error.
         */
        public Counter getRetriesOnOtherErrors() {
            return retriesOnOtherErrors;
        }

        /**
         * Returns the number of times a request was ignored
         * due to the {@link com.datastax.driver.core.policies.RetryPolicy}, for
         * example due to timeouts or unavailability.
         *
         * @return the number of times a request was ignored due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}.
         */
        public Counter getIgnores() {
            return ignores;
        }

        /**
         * Returns the number of times a request was ignored due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * read timed out.
         *
         * @return the number of times a request was ignored due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * read timed out.
         */
        public Counter getIgnoresOnReadTimeout() {
            return ignoresOnReadTimeout;
        }

        /**
         * Returns the number of times a request was ignored due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * write timed out.
         *
         * @return the number of times a request was ignored due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * write timed out.
         */
        public Counter getIgnoresOnWriteTimeout() {
            return ignoresOnWriteTimeout;
        }

        /**
         * Returns the number of times a request was ignored due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after an
         * unavailable exception.
         *
         * @return the number of times a request was ignored due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after an
         * unavailable exception.
         */
        public Counter getIgnoresOnUnavailable() {
            return ignoresOnUnavailable;
        }

        /**
         * Returns the number of times a request was ignored due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * client timeout.
         *
         * @return the number of times a request was ignored due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * client timeout.
         */
        public Counter getIgnoresOnClientTimeout() {
            return ignoresOnClientTimeout;
        }

        /**
         * Returns the number of times a request was ignored due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * connection error.
         *
         * @return the number of times a request was ignored due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after a
         * connection error.
         */
        public Counter getIgnoresOnConnectionError() {
            return ignoresOnConnectionError;
        }

        /**
         * Returns the number of times a request was ignored due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after an
         * unexpected error.
         *
         * @return the number of times a request was ignored due to the
         * {@link com.datastax.driver.core.policies.RetryPolicy}, after an
         * unexpected error.
         */
        public Counter getIgnoresOnOtherErrors() {
            return ignoresOnOtherErrors;
        }

        /**
         * Returns the number of times a speculative execution was started
         * because a previous execution did not complete within the delay
         * specified by {@link SpeculativeExecutionPolicy}.
         *
         * @return the number of speculative executions.
         */
        public Counter getSpeculativeExecutions() {
            return speculativeExecutions;
        }
    }
}
