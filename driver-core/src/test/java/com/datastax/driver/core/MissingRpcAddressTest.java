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

import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * Tests the behavior of the driver when some hosts have no rpc_address in the control host's system tables (JAVA-428).
 * <p/>
 * This can happen because of gossip bugs. We want to ignore these hosts because this is most likely indicative of an error state.
 */
@CreateCCM(PER_METHOD)
@CCMConfig(numberOfNodes = 2, dirtiesContext = true, createCluster = false)
public class MissingRpcAddressTest extends CCMTestsSupport {

    @Test(groups = "short")
    public void testMissingRpcAddressAtStartup() throws Exception {
        deleteNode2RpcAddressFromNode1();
        // Use only one contact point to make sure that the control connection is on node1
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .build());
        cluster.connect();

        // Since node2's RPC address is unknown on our control host, it should have been ignored
        assertEquals(cluster.getMetrics().getConnectedToHosts().getValue().intValue(), 1);
        assertNull(cluster.getMetadata().getHost(getContactPointsWithPorts().get(1)));
    }

    // Artificially modify the system tables to simulate the missing rpc_address.
    private void deleteNode2RpcAddressFromNode1() throws Exception {
        InetSocketAddress firstHost = ccm().addressOfNode(1);
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(firstHost.getAddress())
                .withPort(ccm().getBinaryPort())
                // ensure we will only connect to node1
                .withLoadBalancingPolicy(new WhiteListPolicy(Policies.defaultLoadBalancingPolicy(),
                        Lists.newArrayList(firstHost)))
                .build());
        Session session = cluster.connect();
        String deleteStmt = String.format("DELETE rpc_address FROM system.peers WHERE peer = '%s'",
                ccm().addressOfNode(2).getHostName());
        session.execute(deleteStmt);
        session.close();
        cluster.close();
    }

}
