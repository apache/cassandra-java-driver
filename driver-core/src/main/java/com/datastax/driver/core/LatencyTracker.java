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

/**
 * Interface for object that are interested in tracking the latencies
 * of the driver queries to each Cassandra nodes.
 * <p>
 * An implementaion of this interface can be registered against a Cluster
 * object trough the {@link Cluster#register} method, after which the
 * {@code update} will be called after each query of the driver to a Cassandra
 * host with the latency/duration (in nanoseconds) of this operation.
 */
public interface LatencyTracker {

    /**
     * A method that is called after each request to a Cassandra node with
     * the duration of that operation.
     * <p>
     * Note that there is no guarantee that this method won't be called
     * concurrently by multiple thread, so implementations should synchronize
     * internally if need be.
     *
     * @param host the Cassandra host on which a request has been performed.
     * @param newLatencyNanos the latency in nanoseconds of the operation. This
     * latency corresponds to the time elapsed between when the query was send
     * to {@code host} and when the response was received by the driver (or the
     * operation timeoued, in which {@code newLatencyNanos} will approximately
     * be the timeout value).
     */
    public void update(Host host, long newLatencyNanos);
}
