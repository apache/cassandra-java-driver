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
package com.datastax.driver.mapping;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.LifecycleAwareLatencyTracker;
import com.datastax.driver.core.Statement;
import com.google.common.base.Throwables;
import com.google.common.cache.*;

import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Metrics exposed by the object mapper.
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
public class MapperMetrics {

    static class EntityMetrics {

        // TODO configurable sliding window?
        final Histogram nullFieldsHistogram = new Histogram(new SlidingTimeWindowReservoir(5, MINUTES));

    }

    private final MetricRegistry registry = new MetricRegistry();

    private final LoadingCache<Class<?>, EntityMetrics> metricsByEntity = CacheBuilder.newBuilder()
            .expireAfterAccess(5, MINUTES)
            .removalListener(new RemovalListener<Class<?>, EntityMetrics>() {

                @Override
                public void onRemoval(RemovalNotification<Class<?>, EntityMetrics> notification) {
                    Class<?> entityClass = notification.getKey();
                    if (entityClass != null)
                        registry.remove(entityClass.getName() + "-null-fields");
                }

            })
            .build(new CacheLoader<Class<?>, EntityMetrics>() {

                @Override
                public EntityMetrics load(Class<?> entityClass) throws Exception {
                    EntityMetrics metrics = new EntityMetrics();
                    registry.register(entityClass.getName() + "-null-fields", metrics.nullFieldsHistogram);
                    return metrics;
                }

            });

    private final JmxReporter jmxReporter;

    MapperMetrics(Cluster cluster) {
        if (cluster.getConfiguration().getMetricsOptions().isJMXReportingEnabled()) {
            this.jmxReporter = JmxReporter.forRegistry(registry).inDomain(cluster.getClusterName() + "-mapper-metrics").build();
            this.jmxReporter.start();
            // a bit ugly but works
            // a general-purpose lifecycle-aware thing would be better
            cluster.register(new LifecycleAwareLatencyTracker() {
                @Override
                public void onRegister(Cluster cluster) {
                }

                @Override
                public void onUnregister(Cluster cluster) {
                    jmxReporter.stop();
                }

                @Override
                public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
                }
            });
        } else {
            this.jmxReporter = null;
        }
    }

    public EntityMetrics getEntityMetrics(Class<?> entityClass) {
        EntityMetrics metrics = null;
        try {
            metrics = metricsByEntity.get(entityClass);
        } catch (ExecutionException e) {
            Throwables.propagate(e);
        }
        return metrics;
    }
}
