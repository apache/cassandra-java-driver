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

import org.testng.annotations.Test;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static com.datastax.driver.core.TestUtils.ipOfNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.Mockito.*;

@CreateCCM(PER_METHOD)
public class EventDebouncerIntegrationTest extends CCMTestsSupport {

    /**
     * Tests that DOWN, UP, REMOVE or ADD events will not be delivered to
     * load balancing policy nor host state listeners
     * before the cluster is fully initialized.
     *
     * @throws InterruptedException
     * @jira_ticket JAVA-784
     * @since 2.0.11
     */
    @CCMConfig(numberOfNodes = 3, createCluster = false, dirtiesContext = true)
    @Test(groups = "long")
    public void should_wait_until_load_balancing_policy_is_fully_initialized() throws InterruptedException {
        TestLoadBalancingPolicy policy = new TestLoadBalancingPolicy();
        final Cluster cluster = register(createClusterBuilderNoDebouncing()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .withLoadBalancingPolicy(policy).build());
        new Thread() {
            @Override
            public void run() {
                cluster.init();
            }
        }.start();
        // stop cluster initialization in the middle of LBP initialization
        policy.stop();
        // generate a DOWN event - will not be delivered immediately
        // because the debouncers are not started
        // note: a graceful stop notify other nodes which send a topology change event to the driver right away
        // while forceStop kills the node so other nodes take much more time to detect node failure
        ccm().stop(3);
        ccm().waitForDown(3);
        // finish cluster initialization and deliver the DOWN event
        policy.proceed();
        assertThat(policy.onDownCalledBeforeInit).isFalse();
        assertThat(policy.onDownCalled()).isTrue();
        assertThat(policy.hosts).doesNotContain(TestUtils.findHost(cluster, 3));
    }

    /**
     * Tests that settings for a debouncer can be modified dynamically
     * without requiring the cluster to be restarted.
     *
     * @throws InterruptedException
     * @jira_ticket JAVA-1192
     */
    @CCMConfig(numberOfNodes = 1)
    @Test(groups = "short")
    public void should_change_debouncer_settings_dynamically() throws InterruptedException {
        // Create a spy of the Cluster's control connection and replace it with the spy.
        ControlConnection controlConnection = spy(cluster().manager.controlConnection);
        cluster().manager.controlConnection = controlConnection;
        for (int i = 0; i < 10; i++) {
            cluster().manager.submitNodeListRefresh();
            Thread.sleep(100);
        }
        // all requests should be coalesced into a single one
        verify(controlConnection, timeout(10000)).refreshNodeListAndTokenMap();
        reset(controlConnection);
        // disable debouncing
        cluster().getConfiguration().getQueryOptions()
                .setRefreshNodeListIntervalMillis(0);
        for (int i = 0; i < 10; i++) {
            cluster().manager.submitNodeListRefresh();
            Thread.sleep(100);
        }
        // each request should have been handled separately
        verify(controlConnection, timeout(10000).times(10)).refreshNodeListAndTokenMap();
    }

    private class TestLoadBalancingPolicy extends SortingLoadBalancingPolicy {

        CyclicBarrier stop = new CyclicBarrier(2);

        CyclicBarrier proceed = new CyclicBarrier(2);

        CountDownLatch onDownCalled = new CountDownLatch(1);

        volatile boolean init = false;

        volatile boolean onDownCalledBeforeInit = false;

        @Override
        public void init(Cluster cluster, Collection<Host> hosts) {
            try {
                stop.await(1, TimeUnit.MINUTES);
                proceed.await(1, TimeUnit.MINUTES);
            } catch (Exception e) {
                fail(e.getMessage());
            }
            super.init(cluster, hosts);
            init = true;
        }

        @Override
        public void onDown(Host host) {
            if (!init)
                onDownCalledBeforeInit = true;
            super.onDown(host);
            if (host.getAddress().toString().contains(ipOfNode(3)))
                onDownCalled.countDown();
        }

        void stop() throws InterruptedException {
            try {
                stop.await(1, TimeUnit.MINUTES);
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }

        void proceed() throws InterruptedException {
            try {
                proceed.await(1, TimeUnit.MINUTES);
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }

        boolean onDownCalled() throws InterruptedException {
            return onDownCalled.await(1, TimeUnit.MINUTES);
        }

    }

}
