/*
 *      Copyright (C) 2012 DataStax Inc.
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

import java.util.HashSet;
import java.util.Set;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.JmxReporter;

/**
 * Metrics exposed by the driver.
 * <p>
 * The metrics exposed by this class use the <a href="http://metrics.codahale.com/">Metrics</a>
 * library and you should refer its <a href="http://metrics.codahale.com/manual/">documentation</a>
 * for details on how to handle the exposed metric objects.
 * <p>
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
     * <p>
     * The metrics registry allows you to easily use the reporters that ships
     * with <a href="http://metrics.codahale.com/manual/core/#reporters">Metrics</a>
     * or a custom written one.
     * <p>
     * For instance, if {@code metrics} is {@code this} object, you could export the
     * metrics to csv files using:
     * <pre>
     *     com.codahale.metrics.CsvReporter.forRegistry(metrics.getRegistry()).build(new File("measurements/")).start(1, TimeUnit.SECONDS);
     * </pre>
     *
     * @return the registry containing all metrics.
     */
    public MetricRegistry getRegistry() {
        return registry;
    }

    /**
     * Returns metrics on the user requests performed on the Cluster.
     * <p>
     * This metric exposes
     * <ul>
     *   <li>the total number of requests.</li>
     *   <li>the requests rate (in requests per seconds), including 1, 5 and 15 minute rates.</li>
     *   <li>the mean, min and max latencies, as well as latency at a given percentile.</li>
     * </ul>
     *
     * @return a {@code Timer} metric object exposing the rate and latency for
     * user requests.
     */
    public Timer getRequestsTimer() {
        return requests;
    }

    /**
     * Returns an object regrouping metrics related to the errors encountered.
     *
     * @return an object regrouping metrics related to the errors encountered.
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

        private final Counter otherErrors = registry.counter("other-errors");

        private final Counter retries = registry.counter("retries");
        private final Counter ignores = registry.counter("ignores");

        /**
         * Returns the number of connection to Cassandra nodes errors.
         * <p>
         * This represents the number of times that a request to a Cassandra node
         * has failed due to a connection problem. This thus also corresponds to
         * how often the driver had to pick a fallback host for a request.
         * <p>
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
         * @return the number of times a requests was retried due to the 
         * {@link com.datastax.driver.core.policies.RetryPolicy}.
         */
        public Counter getRetries() {
            return retries;
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
    }
}
