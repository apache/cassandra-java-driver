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

import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.fail;

@CassandraVersion("2.1.0")
public class SingleConnectionPoolTest extends CCMTestsSupport {

    @Test(groups = "short")
    public void should_throttle_requests() {
        // Throttle to a very low value. Even a single thread can generate a higher throughput.
        final int maxRequests = 10;
        cluster().getConfiguration().getPoolingOptions()
                .setMaxRequestsPerConnection(HostDistance.LOCAL, maxRequests);

        // Track in flight requests in a dedicated thread every second
        final AtomicBoolean excessInflightQueriesSpotted = new AtomicBoolean(false);
        final Host host = cluster().getMetadata().getHost(ccm().addressOfNode(1));
        ScheduledExecutorService openConnectionsWatcherExecutor = Executors.newScheduledThreadPool(1);
        final Runnable openConnectionsWatcher = new Runnable() {
            @Override
            public void run() {
                int inFlight = session().getState().getInFlightQueries(host);
                if (inFlight > maxRequests)
                    excessInflightQueriesSpotted.set(true);
            }
        };
        openConnectionsWatcherExecutor.scheduleAtFixedRate(openConnectionsWatcher, 200, 200, TimeUnit.MILLISECONDS);

        // Generate the load
        for (int i = 0; i < 10000; i++)
            session().executeAsync("SELECT release_version FROM system.local");

        openConnectionsWatcherExecutor.shutdownNow();
        if (excessInflightQueriesSpotted.get()) {
            fail("Inflight queries exceeded the limit");
        }
    }
}
