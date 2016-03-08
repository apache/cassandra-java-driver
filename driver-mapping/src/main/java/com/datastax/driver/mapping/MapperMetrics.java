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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.google.common.base.Throwables;
import com.google.common.cache.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

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
 * <p>
 * To disable metrics for the mapper altogether, set the system property
 * <code>{@value DISABLE_METRICS_KEY}</code> to {@code "true"}.
 */
public class MapperMetrics {

    public static final String DISABLE_METRICS_KEY = "com.datastax.driver.mapping.DISABLE_METRICS";

    /**
     * A simple sliding time window that keeps a global counter of events
     * happened in the last N minutes (or whichever time unit is used.
     * <p>
     * The window is backed by an {@link AtomicReferenceArray} that acts
     * like a circular buffer and stores individual counter for each buffer slot.
     * <p>
     * The accuracy depends on the buffer size, and hence on the window parameters.
     * The more time slots, the more accurate it is.
     * <p>
     * Unfortunately implementing Counting is not enough, we need to extend Counter.
     * See https://github.com/dropwizard/metrics/issues/703.
     */
    public static class SlidingTimeWindowCounter extends Counter implements Metric, Counting {

        private static class TimeSlot {

            final long expiresAt;

            final long count;

            TimeSlot(long expiresAt, long count) {
                this.expiresAt = expiresAt;
                this.count = count;
            }

        }

        private final AtomicReferenceArray<TimeSlot> window;

        private final long windowSizeMillis;

        private final long slotSizeMillis;

        /**
         * Creates a new counter with the specified {@code windowSize}
         * and the specified {@code slotSize}; the ratio {@code windowSize / slotSize}
         * determines the accuracy of the counter: the higher the ratio is, the more
         * accurate the counter will be.
         *
         * @param windowSize the window size in time units.
         * @param slotSize   the slot size in time units.
         * @param unit       the time unit to use.
         */
        public SlidingTimeWindowCounter(long windowSize, long slotSize, TimeUnit unit) {
            windowSizeMillis = unit.toMillis(windowSize);
            slotSizeMillis = unit.toMillis(slotSize);
            window = new AtomicReferenceArray<TimeSlot>((int) (windowSize / slotSize));
        }

        public long incrementAndGetCount() {
            return incrementAndGetCount(1);
        }

        @Override
        public long getCount() {
            return incrementAndGetCount(0);
        }

        @Override
        public void inc() {
            incrementAndGetCount(1);
        }

        @Override
        public void inc(long n) {
            incrementAndGetCount(n);
        }

        @Override
        public void dec() {
            incrementAndGetCount(-1);
        }

        @Override
        public void dec(long n) {
            incrementAndGetCount(-n);
        }

        private long incrementAndGetCount(long increment) {
            long count = 0;
            long now = System.currentTimeMillis();
            int slotToUpdate = increment == 0 ? -1 : (int) ((now / slotSizeMillis) % window.length());
            for (int i = 0; i < window.length(); i++) {
                TimeSlot current;
                TimeSlot update = null;
                do {
                    current = window.get(i);
                    if (i == slotToUpdate) {
                        if (current == null || now >= current.expiresAt)
                            // slot expired, reset counter
                            update = new TimeSlot(now + windowSizeMillis, 1);
                        else
                            update = new TimeSlot(current.expiresAt, current.count + 1);
                    } else {
                        if (current != null && now < current.expiresAt)
                            update = current;
                    }
                } while (i == slotToUpdate && !window.compareAndSet(i, current, update));
                if (update != null)
                    count += update.count;
            }
            return count;
        }
    }

    /**
     * Statistics and metrics about a given entity.
     */
    public static class EntityMetrics {

        private final Class<?> entityClass;

        private final SlidingTimeWindowCounter nullFieldsCounter;

        public EntityMetrics(Class<?> entityClass, SlidingTimeWindowCounter nullFieldsCounter) {
            this.entityClass = entityClass;
            this.nullFieldsCounter = nullFieldsCounter;
        }

        /**
         * @return The entity class.
         */
        public Class<?> getEntityClass() {
            return entityClass;
        }

        public SlidingTimeWindowCounter getNullFieldsCounter() {
            return nullFieldsCounter;
        }

    }

    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    private class EntityMetricsLoader extends CacheLoader<Class<?>, EntityMetrics> {

        @Override
        public EntityMetrics load(Class<?> entityClass) throws Exception {
            // TODO configurable sliding window?
            // default is 5 minutes with slots of 10 seconds (30 slots)
            EntityMetrics metrics = new EntityMetrics(entityClass, new SlidingTimeWindowCounter(300, 10, SECONDS));
            registry.register(String.format("mapper-%s-%s-null-fields", id, entityClass.getName()), metrics.nullFieldsCounter);
            return metrics;
        }

    }

    private class EntityMetricsRemovalListener implements RemovalListener<Class<?>, EntityMetrics> {

        @Override
        public void onRemoval(RemovalNotification<Class<?>, EntityMetrics> notification) {
            Class<?> entityClass = notification.getKey();
            if (entityClass != null)
                registry.remove(String.format("mapper-%s-%s-null-fields", id, entityClass.getName()));
        }

    }

    private final MetricRegistry registry;

    // distinguish between different instances of this class
    private final int id;

    private final LoadingCache<Class<?>, EntityMetrics> metricsByEntity = CacheBuilder.newBuilder()
            .expireAfterAccess(5, MINUTES)
            .removalListener(new EntityMetricsRemovalListener())
            .build(new EntityMetricsLoader());

    MapperMetrics(Cluster cluster) {
        registry = cluster.getMetrics().getRegistry();
        id = COUNTER.incrementAndGet();
    }

    /**
     * Gathers metrics about the given entity class.
     *
     * @param entityClass the entity class to gather metrics for.
     * @return An {@link EntityMetrics} with metrics for the given entity class.
     */
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
