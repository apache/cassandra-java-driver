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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

import com.datastax.driver.core.policies.DelegatingLoadBalancingPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;

public class LoadBalancingPolicyBootstrapTest {
    CountingPolicy policy;
    CCMBridge.CCMCluster c;

    @BeforeClass(groups = "short")
    private void setup() {
        policy = new CountingPolicy(new RoundRobinPolicy());
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(policy);
        c = CCMBridge.buildCluster(2, builder);
    }

    @Test(groups = "short")
    public void notificationsTest() throws Exception {
        assertEquals(policy.inits, 2, "inits\n" + policy.history);
        assertEquals(policy.adds, 0, "adds\n" + policy.history);
        assertEquals(policy.suspecteds, 0, "suspecteds\n" + policy.history);
        assertEquals(policy.removes, 0, "removes\n" + policy.history);
        assertEquals(policy.ups, 0, "ups\n" + policy.history);
        assertEquals(policy.downs, 0, "downs\n" + policy.history);
    }

    @AfterClass(groups = "short")
    private void tearDown() {
        c.discard();
    }

    static class CountingPolicy extends DelegatingLoadBalancingPolicy {

        int inits;
        int adds;
        int suspecteds;
        int removes;
        int ups;
        int downs;

        final StringWriter history = new StringWriter();
        private final PrintWriter out = new PrintWriter(history);

        public CountingPolicy(LoadBalancingPolicy delegate) {
            super(delegate);
        }

        @Override
        public void init(Cluster cluster, Collection<Host> hosts) {
            super.init(cluster, hosts);
            inits += hosts.size();
        }

        public void onAdd(Host host) {
            out.printf("add %s%n", host);
            adds++;
            super.onAdd(host);
        }

        public void onSuspected(Host host) {
            out.printf("suspect %s%n", host);
            suspecteds++;
            super.onSuspected(host);
        }

        public void onUp(Host host) {
            out.printf("up %s%n", host);
            ups++;
            super.onUp(host);
        }

        public void onDown(Host host) {
            out.printf("down %s%n", host);
            downs++;
            super.onDown(host);
        }

        public void onRemove(Host host) {
            out.printf("remove %s%n", host);
            removes++;
            super.onRemove(host);
        }
    }
}
