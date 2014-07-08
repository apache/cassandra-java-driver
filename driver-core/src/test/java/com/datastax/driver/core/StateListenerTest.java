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

import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

import static com.datastax.driver.core.TestUtils.*;

/**
 * Simple test of the Sessions methods against a one node cluster.
 */
public class StateListenerTest {

    @Test(groups = "short")
    public void listenerTest() throws Throwable {

        CCMBridge.CCMCluster c = CCMBridge.buildCluster(1, Cluster.builder());
        Cluster cluster = c.cluster;

        try {
            CountingListener listener = new CountingListener();
            cluster.register(listener);

            c.cassandraCluster.bootstrapNode(2);
            waitFor(CCMBridge.IP_PREFIX + '2', cluster);

            // We sleep slightly before checking the listener because the node is marked UP
            // just before the listeners are called, and since waitFor is based on isUP,
            // it can return *just* before the listener are called.
            Thread.sleep(500);
            assertEquals(listener.adds, 1);

            c.cassandraCluster.forceStop(1);
            waitForDown(CCMBridge.IP_PREFIX + '1', cluster);
            Thread.sleep(500);
            assertEquals(listener.downs, 1);

            c.cassandraCluster.start(1);
            waitFor(CCMBridge.IP_PREFIX + '1', cluster);
            Thread.sleep(500);
            assertEquals(listener.ups, 1);

            c.cassandraCluster.decommissionNode(2);
            waitForDecommission(CCMBridge.IP_PREFIX + '2', cluster);
            Thread.sleep(500);
            assertEquals(listener.removes, 1);
        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            c.discard();
        }
    }

    private static class CountingListener implements Host.StateListener {

        public int adds;
        public int removes;
        public int ups;
        public int downs;

        public void onAdd(Host host) {
            adds++;
        }

        public void onUp(Host host) {
            ups++;
        }

        public void onSuspected(Host host) {
        }

        public void onDown(Host host) {
            downs++;
        }

        public void onRemove(Host host) {
            removes++;
        }
    }
}
