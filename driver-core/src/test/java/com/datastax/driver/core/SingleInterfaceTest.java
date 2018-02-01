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

import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static com.datastax.driver.core.TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Test limitations when using large amounts of data with the driver
 */
public class SingleInterfaceTest {

    /**
     * Test a wide row of size 1,000,000
     */
    @Test(groups = "short")
    @CassandraVersion(value = "4.0.0", description = "< 4.0 is skipped as a single interface cluster is unsupported.")
    public void testSingleInterface() throws Throwable {

        CCMBridge bridge = CCMBridge.builder().withNodes(3).useSingleInterface().build();
        try
        {
            Cluster cluster = Cluster.builder().addContactPoint("127.0.1.1:9046").allowServerPortDiscovery().build();
            SessionManager session = (SessionManager)cluster.connect();
            Set<Host> hosts = cluster.getMetadata().getAllHosts();
            InetAddress intf = null;
            Set<Integer> rpcPorts = new HashSet<Integer>();
            Set<Integer> broadcastPorts = new HashSet<Integer>();

            assertTrue(cluster.manager.controlConnection.isOpen());

            for(Host h : hosts)
            {
                if (intf == null)
                {
                    intf = h.getSocketAddress().getAddress();
                }
                else
                {
                    assertEquals(intf, h.getSocketAddress().getAddress());
                }

                assertTrue(rpcPorts.add(h.getSocketAddress().getPort()));
                assertTrue(broadcastPorts.add(h.getBroadcastAddressOptPort().getPort()));
            }
            assertEquals(3, cluster.getMetadata().getAllHosts().size());
            assertEquals(3, session.pools.size());
            for (HostConnectionPool pool : session.pools.values())
            {
                assertEquals(1, pool.connections.size());
                for (Connection c : pool.connections)
                {
                    assertEquals(Connection.State.OPEN, c.state.get());
                }
                assertTrue(pool.host.isUp());
            }

            List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();
            for (int ii = 0; ii < 100; ii++)
            {
                 results.add(session.executeAsync(new SimpleStatement("select * from system_distributed.view_build_status").setConsistencyLevel(ConsistencyLevel.ALL)));
            }
            for (ResultSetFuture result : results) {
                result.getUninterruptibly();
            }
        }
        finally
        {
            bridge.close();
        }
    }
}
