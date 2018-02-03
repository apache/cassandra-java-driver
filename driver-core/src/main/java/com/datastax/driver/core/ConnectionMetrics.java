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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionMetrics {

    private static final String METRICS_PREFIX = "cassandra.";
    private static final Logger logger = LoggerFactory.getLogger(ConnectionMetrics.class);

    public ConnectionMetrics(Session session, MetricRegistry metricRegistry) {
        logInitialConfiguration(session);
        registerMetrics(session, metricRegistry);
    }

    private void logInitialConfiguration(Session session) {
        PoolingOptions poolingOptions = session.getCluster().getConfiguration().getPoolingOptions();
        logger.info("maxConnPerLocalHost={}", poolingOptions.getMaxConnectionsPerHost(HostDistance.LOCAL));
        logger.info("maxConnPerRemoteHost={}", poolingOptions.getMaxConnectionsPerHost(HostDistance.REMOTE));
        logger.info("localConnThreshold={}", poolingOptions.getNewConnectionThreshold(HostDistance.LOCAL));
        logger.info("remoteConnThreshold={}", poolingOptions.getNewConnectionThreshold(HostDistance.REMOTE));
        logger.info("maxReqPerLocalHost={}", poolingOptions.getMaxRequestsPerConnection(HostDistance.LOCAL));
        logger.info("maxReqPerRemoteHost={}", poolingOptions.getMaxRequestsPerConnection(HostDistance.REMOTE));
    }

    private void registerMetrics(final Session session, final MetricRegistry metricRegistry) {
        session.getCluster().register(new CassandraStateListener());
        Metrics metrics = session.getCluster().getMetrics();
        Metrics.Errors errorMetrics = metrics.getErrorMetrics();

        metricRegistry.register(METRICS_PREFIX + "retries", errorMetrics.getRetries());
        metricRegistry.register(METRICS_PREFIX + "retries-on-read-timeout", errorMetrics.getRetriesOnReadTimeout());
        metricRegistry.register(METRICS_PREFIX + "retries-on-unavailable", errorMetrics.getRetriesOnUnavailable());
        metricRegistry.register(METRICS_PREFIX + "unavailables", errorMetrics.getUnavailables());
        metricRegistry.register(METRICS_PREFIX + "ignores", errorMetrics.getIgnores());
        metricRegistry.register(METRICS_PREFIX + "connection-errors", errorMetrics.getConnectionErrors());
        metricRegistry.register(METRICS_PREFIX + "speculative-executions", errorMetrics.getSpeculativeExecutions());
        metricRegistry.register(METRICS_PREFIX + "read-timeouts", errorMetrics.getReadTimeouts());
        metricRegistry.register(METRICS_PREFIX + "requests", metrics.getRequestsTimer());

        for (final Host host : session.getState().getConnectedHosts()) {
            String hostName = sanitizeHostName(host);

            metricRegistry
                    .register(METRICS_PREFIX + "openConnections." + hostName, (Gauge) new Gauge() {
                        @Override
                        public Object getValue() {
                            return session.getState().getOpenConnections(host);
                        }
                    });
            metricRegistry.register(METRICS_PREFIX + "trashedConnections." + hostName,
                    (Gauge) new Gauge() {
                        @Override
                        public Object getValue() {
                            return session.getState().getTrashedConnections(host);
                        }
                    });
            metricRegistry
                    .register(METRICS_PREFIX + "inFlightQueries." + hostName, (Gauge) new Gauge() {
                        @Override
                        public Object getValue() {
                            return session.getState().getInFlightQueries(host);
                        }
                    });
        }
    }

    private String sanitizeHostName(Host host) {
        return host.toString().replaceAll("[:/.]", "_");
    }

    class CassandraStateListener implements Host.StateListener {
        @Override
        public void onAdd(Host host) {
            logger.info(METRICS_PREFIX + "onAdd: host [name={}, address={}]", host.getAddress().getHostName(), host.getAddress().getHostAddress());
        }

        @Override
        public void onUp(Host host) {
            logger.info(METRICS_PREFIX + "onUp: host [name={}, address={}]", host.getAddress().getHostName(), host.getAddress().getHostAddress());
        }

        @Override
        public void onDown(Host host) {
            logger.info(METRICS_PREFIX + "onDown: host [name={}, address={}]", host.getAddress().getHostName(), host.getAddress().getHostAddress());
        }

        @Override
        public void onRemove(Host host) {
            logger.info(METRICS_PREFIX + "onRemove: host [name={}, address={}]", host.getAddress().getHostName(), host.getAddress().getHostAddress());
        }

        @Override
        public void onRegister(Cluster cluster) {
            logger.info(METRICS_PREFIX + "onRegister: cluster [name={}]", cluster.getClusterName());
        }

        @Override
        public void onUnregister(Cluster cluster) {
            logger.info(METRICS_PREFIX + "onUnregister: cluster [name={}]", cluster.getClusterName());
        }
    }
}