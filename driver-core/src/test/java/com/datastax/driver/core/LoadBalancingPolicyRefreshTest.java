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

import java.net.InetAddress;
import java.util.*;

import com.google.common.collect.Iterators;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

import com.datastax.driver.core.policies.LoadBalancingPolicy;

// Test that PoolingOpions.refreshConnectedHosts works as expected (JAVA-309)
public class LoadBalancingPolicyRefreshTest {

    private static class UpdatablePolicy implements LoadBalancingPolicy {

        private Cluster cluster;
        private Host theHost;

        public void changeTheHost(Host theNewHost) {
            this.theHost = theNewHost;
            cluster.getConfiguration().getPoolingOptions().refreshConnectedHosts();
        }

        public void init(Cluster cluster, Collection<Host> hosts) {
            this.cluster = cluster;
            try {
                for (Host h : hosts)
                    if (h.getAddress().equals(InetAddress.getByName(CCMBridge.IP_PREFIX + '1')))
                        this.theHost = h;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public HostDistance distance(Host host) {
            return host == theHost ? HostDistance.LOCAL : HostDistance.IGNORED;
        }

        public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            return Iterators.<Host>singletonIterator(theHost);
        }

        public void onAdd(Host h) {}
        public void onRemove(Host h) {}
        public void onUp(Host h) {}
        public void onDown(Host h) {}
        public void onSuspected(Host h) {}
    }

    @Test(groups = "short")
    public void refreshTest() throws Throwable {

        UpdatablePolicy policy = new UpdatablePolicy();
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(policy);
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, builder);
        try {

            Session s = c.session;

            // Ugly
            Host[] hosts = new Host[2];
            for (Host h : c.cluster.getMetadata().getAllHosts()) {
                if (h.getAddress().equals(InetAddress.getByName(CCMBridge.IP_PREFIX + '1')))
                    hosts[0] = h;
                else
                    hosts[1] = h;
            }

            assertTrue(s.getState().getConnectedHosts().contains(hosts[0]), "Connected hosts: " + s.getState().getConnectedHosts());
            assertTrue(!s.getState().getConnectedHosts().contains(hosts[1]), "Connected hosts: " + s.getState().getConnectedHosts());

            policy.changeTheHost(hosts[1]);

            assertTrue(!s.getState().getConnectedHosts().contains(hosts[0]), "Connected hosts: " + s.getState().getConnectedHosts());
            assertTrue(s.getState().getConnectedHosts().contains(hosts[1]), "Connected hosts: " + s.getState().getConnectedHosts());

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            c.discard();
        }
    }
}
