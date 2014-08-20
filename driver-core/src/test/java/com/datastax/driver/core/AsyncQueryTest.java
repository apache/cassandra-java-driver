package com.datastax.driver.core;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AsyncQueryTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList();
    }

    /**
     * Checks that a cancelled query releases the connection (JAVA-407).
     */
    @Test(groups = "short")
    public void cancelQueryTest() throws InterruptedException {
        ResultSetFuture future = session.executeAsync("select release_version from system.local");
        future.cancel(true);
        assertTrue(future.isCancelled());

        TimeUnit.MILLISECONDS.sleep(100);

        HostConnectionPool pool = getPool(session);
        for (Connection connection : pool.connections) {
            assertEquals(connection.inFlight.get(), 0);
        }
    }

    private static HostConnectionPool getPool(Session session) {
        Collection<HostConnectionPool> pools = ((SessionManager) session).pools.values();
        assertEquals(pools.size(), 1);
        return pools.iterator().next();
    }
}
