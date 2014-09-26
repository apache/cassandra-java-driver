/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

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
        if (pool instanceof DynamicConnectionPool) {
            DynamicConnectionPool p = (DynamicConnectionPool)pool;
            for (Connection connection : p.connections) {
                assertEquals(connection.inFlight.get(), 0);
            }
        } else if (pool instanceof SingleConnectionPool){
            SingleConnectionPool p = (SingleConnectionPool)pool;
            assertEquals(p.connectionRef.get().inFlight.get(), 0);
        }
    }

    private static HostConnectionPool getPool(Session session) {
        Collection<HostConnectionPool> pools = ((SessionManager) session).pools.values();
        assertEquals(pools.size(), 1);
        return pools.iterator().next();
    }
}
