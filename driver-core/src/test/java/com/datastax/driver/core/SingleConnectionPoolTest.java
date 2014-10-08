package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.Assert.fail;

public class SingleConnectionPoolTest extends CCMBridge.PerClassSingleNodeCluster {
    @Override
    protected Collection<String> getTableDefinitions() {
        TestUtils.versionCheck(2.1, 0, "This will only work with Cassandra 2.1.0+");

        return Lists.newArrayList();
    }

    @Test(groups = "short")
    public void should_throttle_requests() {
        // Throttle to a very low value. Even a single thread can generate a higher throughput.
        final int maxRequests = 10;
        cluster.getConfiguration().getPoolingOptions()
               .setMaxSimultaneousRequestsPerHostThreshold(HostDistance.LOCAL, maxRequests);

        // Track in flight requests in a dedicated thread every second
        final AtomicBoolean excessInflightQueriesSpotted = new AtomicBoolean(false);
        final Host host = cluster.getMetadata().getHost(new InetSocketAddress(CCMBridge.IP_PREFIX + "1", 9042));
        ScheduledExecutorService openConnectionsWatcherExecutor = Executors.newScheduledThreadPool(1);
        final Runnable openConnectionsWatcher = new Runnable() {
            @Override
            public void run() {
                int inFlight = session.getState().getInFlightQueries(host);
                if (inFlight > maxRequests)
                    excessInflightQueriesSpotted.set(true);
            }
        };
        openConnectionsWatcherExecutor.scheduleAtFixedRate(openConnectionsWatcher, 200, 200, TimeUnit.MILLISECONDS);

        // Generate the load
        for (int i = 0; i < 10000; i++)
            session.executeAsync("SELECT release_version FROM system.local");

        openConnectionsWatcherExecutor.shutdownNow();
        if (excessInflightQueriesSpotted.get()) {
            fail("Inflight queries exceeded the limit");
        }
    }
}
