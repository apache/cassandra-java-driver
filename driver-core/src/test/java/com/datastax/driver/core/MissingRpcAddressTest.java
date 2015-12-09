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

import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * Tests the behavior of the driver when some hosts have no rpc_address in the control host's system tables (JAVA-428).
 * <p/>
 * This can happen because of gossip bugs. We want to ignore these hosts because this is most likely indicative of an error state.
 */
public class MissingRpcAddressTest {

    @Test(groups = "short")
    public void testMissingRpcAddressAtStartup() throws Exception {
        CCMBridge ccm = CCMBridge.builder("ccm").withNodes(2).build();

        Cluster cluster = null;
        try {
            deleteNode2RpcAddressFromNode1();
            // Use only one contact point to make sure that the control connection is on node1
            cluster = Cluster.builder()
                    .addContactPoint(CCMBridge.IP_PREFIX + "1")
                    .build();
            cluster.connect();

            // Since node2's RPC address is unknown on our control host, it should have been ignored
            assertEquals(cluster.getMetrics().getConnectedToHosts().getValue().intValue(), 1);
            assertNull(cluster.getMetadata().getHost(socketAddress(2)));
        } finally {
            if (cluster != null)
                cluster.close();
            ccm.remove();
        }
    }

    // Artificially modify the system tables to simulate the missing rpc_address.
    private void deleteNode2RpcAddressFromNode1() throws Exception {
        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                    .addContactPoint(CCMBridge.IP_PREFIX + "1")
                            // ensure we will only connect to node1
                    .withLoadBalancingPolicy(new WhiteListPolicy(Policies.defaultLoadBalancingPolicy(),
                            Lists.newArrayList(socketAddress(1))))
                    .build();
            Session session = cluster.connect();
            String deleteStmt = String.format("DELETE rpc_address FROM system.peers WHERE peer = '%s'",
                    InetAddress.getByName(CCMBridge.IP_PREFIX + "2").getHostName());
            session.execute(deleteStmt);
            session.close();
        } finally {
            if (cluster != null)
                cluster.close();
        }
    }

    private static InetSocketAddress socketAddress(int node) {
        return new InetSocketAddress(CCMBridge.IP_PREFIX + Integer.toString(node), 9042);
    }
}
