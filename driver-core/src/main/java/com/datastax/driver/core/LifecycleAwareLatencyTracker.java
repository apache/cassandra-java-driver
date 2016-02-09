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

/**
 * A {@code LatencyTracker} that tracks when it gets registered or unregistered with a cluster.
 * <p/>
 * This interface exists only for backward-compatibility reasons: starting with the 3.0 branch of the driver, its
 * methods are on the parent interface directly.
 */
public interface LifecycleAwareLatencyTracker extends LatencyTracker {
    /**
     * Gets invoked when the tracker is registered with a cluster, or at cluster startup if the
     * tracker was registered at initialization with
     * {@link com.datastax.driver.core.Cluster.Initializer#register(LatencyTracker)}.
     *
     * @param cluster the cluster that this tracker is registered with.
     */
    void onRegister(Cluster cluster);

    /**
     * Gets invoked when the tracker is unregistered from a cluster, or at cluster shutdown if
     * the tracker was not unregistered.
     *
     * @param cluster the cluster that this tracker was registered with.
     */
    void onUnregister(Cluster cluster);
}
