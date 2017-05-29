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

import com.google.common.base.Throwables;
import com.google.common.cache.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class CCMCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(CCMCache.class);

    private static class CachedCCMAccess implements CCMAccess {

        private final CCMAccess ccm;

        private final AtomicInteger refCount = new AtomicInteger(1);

        private volatile boolean evicted = false;

        private CachedCCMAccess(CCMAccess ccm) {
            this.ccm = ccm;
        }

        @Override
        public String getClusterName() {
            return ccm.getClusterName();
        }

        @Override
        public VersionNumber getCassandraVersion() {
            return ccm.getCassandraVersion();
        }

        @Override
        public VersionNumber getDSEVersion() {
            return ccm.getDSEVersion();
        }

        @Override
        public File getCcmDir() {
            return ccm.getCcmDir();
        }

        @Override
        public File getClusterDir() {
            return ccm.getClusterDir();
        }

        @Override
        public File getNodeDir(int n) {
            return ccm.getNodeDir(n);
        }

        @Override
        public File getNodeConfDir(int n) {
            return ccm.getNodeConfDir(n);
        }

        @Override
        public int getStoragePort() {
            return ccm.getStoragePort();
        }

        @Override
        public int getThriftPort() {
            return ccm.getThriftPort();
        }

        @Override
        public int getBinaryPort() {
            return ccm.getBinaryPort();
        }

        @Override
        public void setKeepLogs(boolean keepLogs) {
            ccm.setKeepLogs(keepLogs);
        }

        @Override
        public InetSocketAddress addressOfNode(int n) {
            return ccm.addressOfNode(n);
        }

        @Override
        public void start() {
            ccm.start();
        }

        @Override
        public void stop() {
            ccm.stop();
        }

        @Override
        public void forceStop() {
            ccm.forceStop();
        }

        @Override
        public void close() {
            refCount.decrementAndGet();
            maybeClose();
        }

        private void maybeClose() {
            if (refCount.get() <= 0 && evicted) {
                ccm.close();
            }
        }

        @Override
        public void remove() {
            ccm.remove();
        }

        @Override
        public void updateConfig(Map<String, Object> configs) {
            ccm.updateConfig(configs);
        }

        @Override
        public void updateDSEConfig(Map<String, Object> configs) {
            ccm.updateDSEConfig(configs);
        }

        @Override
        public String checkForErrors() {
            return ccm.checkForErrors();
        }

        @Override
        public void start(int n) {
            ccm.start(n);
        }

        @Override
        public void stop(int n) {
            ccm.stop(n);
        }

        @Override
        public void forceStop(int n) {
            ccm.forceStop(n);
        }

        @Override
        public void pause(int n) {
            ccm.pause(n);
        }

        @Override
        public void resume(int n) {
            ccm.resume(n);
        }

        @Override
        public void remove(int n) {
            ccm.remove(n);
        }

        @Override
        public void add(int n) {
            ccm.add(n);
        }

        @Override
        public void add(int dc, int n) {
            ccm.add(dc, n);
        }

        @Override
        public void decommission(int n) {
            ccm.decommission(n);
        }

        @Override
        public void updateNodeConfig(int n, String key, Object value) {
            ccm.updateNodeConfig(n, key, value);
        }

        @Override
        public void updateNodeConfig(int n, Map<String, Object> configs) {
            ccm.updateNodeConfig(n, configs);
        }

        @Override
        public void updateDSENodeConfig(int n, String key, Object value) {
            ccm.updateDSENodeConfig(n, key, value);
        }

        @Override
        public void updateDSENodeConfig(int n, Map<String, Object> configs) {
            ccm.updateDSENodeConfig(n, configs);
        }

        @Override
        public void setWorkload(int n, Workload... workload) {
            ccm.setWorkload(n, workload);
        }

        @Override
        public void waitForUp(int node) {
            ccm.waitForUp(node);
        }

        @Override
        public void waitForDown(int node) {
            ccm.waitForDown(node);
        }

        @Override
        public ProtocolVersion getProtocolVersion() {
            return ccm.getProtocolVersion();
        }

        @Override
        public ProtocolVersion getProtocolVersion(ProtocolVersion maximumAllowed) {
            return ccm.getProtocolVersion(maximumAllowed);
        }

        @Override
        public String toString() {
            return ccm.toString();
        }
    }

    private static class CCMAccessLoader extends CacheLoader<CCMBridge.Builder, CachedCCMAccess> {

        @Override
        public CachedCCMAccess load(CCMBridge.Builder key) {
            return new CachedCCMAccess(key.build());
        }

    }

    private static class CCMAccessWeigher implements Weigher<CCMBridge.Builder, CachedCCMAccess> {

        @Override
        public int weigh(CCMBridge.Builder key, CachedCCMAccess value) {
            return key.weight();
        }

    }

    private static class CCMAccessRemovalListener implements RemovalListener<CCMBridge.Builder, CachedCCMAccess> {

        @Override
        public void onRemoval(RemovalNotification<CCMBridge.Builder, CachedCCMAccess> notification) {
            CachedCCMAccess cached = notification.getValue();
            if (cached != null && cached.ccm != null) {
                LOGGER.debug("Evicting: {}, reason: {}", cached.ccm, notification.getCause());
                cached.evicted = true;
                cached.maybeClose();
            }
        }

    }

    /**
     * A LoadingCache that stores running CCM clusters.
     */
    private static final LoadingCache<CCMBridge.Builder, CachedCCMAccess> CACHE;

    // The amount of memory one CCM node takes in MB.
    private static final int ONE_CCM_NODE_MB = 800;

    static {
        long maximumWeight;
        String numberOfNodes = System.getProperty("ccm.maxNumberOfNodes");
        if (numberOfNodes == null) {
            long freeMemoryMB = TestUtils.getFreeMemoryMB();
            if (freeMemoryMB < ONE_CCM_NODE_MB)
                LOGGER.warn("Not enough available memory: {} MB, CCM clusters might fail to start", freeMemoryMB);
            // CCM nodes are started with -Xms500M -Xmx500M
            // and allocate up to 100MB non-heap memory in the general case,
            // to be conservative we treat 1 "slot" as 800Mb.
            // We leave 3 slots out to avoid starving system memory,
            // and we pick a value with a minimum of 1 slot and a maximum of 8 slots.
            // For example, an 8GB VM with ~6.5GB currently available heap will yield 5 slots ((6500/800) - 3 = 5).
            long slotsAvailable = (freeMemoryMB / ONE_CCM_NODE_MB) - 3;
            maximumWeight = Math.min(8, Math.max(1, slotsAvailable));
        } else {
            maximumWeight = Integer.parseInt(numberOfNodes);
        }
        LOGGER.info("Maximum number of running CCM nodes: {}", maximumWeight);
        CACHE = CacheBuilder.newBuilder()
                .initialCapacity(3)
                .softValues()
                .maximumWeight(maximumWeight)
                .weigher(new CCMAccessWeigher())
                .removalListener(new CCMAccessRemovalListener())
                .recordStats()
                .build(new CCMAccessLoader());
    }

    /**
     * Creates or recycles a {@link CCMAccess} instance and returns it.
     * <p/>
     * Caller MUST call {@link CCMAccess#close()} when done with the cluster,
     * to ensure that resources will be properly freed.
     */
    public static CCMAccess get(CCMBridge.Builder key) {
        CachedCCMAccess ccm = CACHE.getIfPresent(key);
        if (ccm != null) {
            ccm.refCount.incrementAndGet();
        } else {
            try {
                ccm = CACHE.get(key);
            } catch (ExecutionException e) {
                throw Throwables.propagate(e);
            }
        }
        logCache();
        return ccm;
    }

    /**
     * Removes the given key from the cache.
     */
    public static void remove(CCMBridge.Builder key) {
        CACHE.invalidate(key);
    }

    private static void logCache() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Free memory: {} MB", TestUtils.getFreeMemoryMB());
            StringBuilder sb = new StringBuilder();
            Iterator<Map.Entry<CCMBridge.Builder, CachedCCMAccess>> iterator = CACHE.asMap().entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<CCMBridge.Builder, CachedCCMAccess> entry = iterator.next();
                sb.append(entry.getValue().getClusterName())
                        .append(" (")
                        .append(entry.getKey().weight())
                        .append(")");
                if (iterator.hasNext())
                    sb.append(", ");
            }
            LOGGER.debug("Cache contents: {{}}", sb.toString());
            LOGGER.debug("Cache stats: {}", CACHE.stats());
        }
    }

}
