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

import com.datastax.driver.core.Host.State;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.LimitingLoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;

@CCMConfig(
        dirtiesContext = true,
        numberOfNodes = 2,
        createCluster = false
)
public class RefreshConnectedHostTest extends CCMTestsSupport {

    /**
     * Tests {@link PoolingOptions#refreshConnectedHost(Host)} through a custom load balancing policy.
     */
    @Test(groups = "long")
    public void should_refresh_single_connected_host() {
        // This will make the driver use at most 2 hosts, the others will be ignored
        LimitingLoadBalancingPolicy loadBalancingPolicy = new LimitingLoadBalancingPolicy(new RoundRobinPolicy(), 2, 1);

        PoolingOptions poolingOptions = Mockito.spy(new PoolingOptions());
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .withPoolingOptions(poolingOptions)
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(1000))
                .withQueryOptions(TestUtils.nonDebouncingQueryOptions())
                .build());

        Session session = cluster.connect();

        assertThat(cluster).usesControlHost(1);
        assertThat(cluster).host(1)
                .hasState(State.UP)
                .isAtDistance(HostDistance.LOCAL);
        // Wait for the node to be up, because apparently on Jenkins it's still only ADDED when we reach this line
        // Waiting for NEW_NODE_DELAY_SECONDS+1 allows the driver to create a connection pool and mark the node up
        assertThat(cluster).host(2)
                .comesUpWithin(Cluster.NEW_NODE_DELAY_SECONDS + 1, SECONDS)
                .isAtDistance(HostDistance.LOCAL);

        // Add and bring host 3 up, its presence should be acknowledged but it should be ignored
        ccm().add(3);
        ccm().start(3);
        ccm().waitForUp(3);

        assertThat(cluster).host(1)
                .hasState(State.UP)
                .isAtDistance(HostDistance.LOCAL);
        assertThat(cluster).host(2)
                .hasState(State.UP)
                .isAtDistance(HostDistance.LOCAL);

        // Ensure that the host is added to the Cluster.
        assertThat(cluster).host(3)
                .comesUpWithin(Cluster.NEW_NODE_DELAY_SECONDS + 1, SECONDS)
                .isAtDistance(HostDistance.IGNORED);
        assertThat(session).hasNoPoolFor(3);

        // Kill host 2, host 3 should take its place
        ccm().stop(2);
        TestUtils.waitForUp(TestUtils.ipOfNode(3), cluster);

        assertThat(cluster).host(1)
                .hasState(State.UP)
                .isAtDistance(HostDistance.LOCAL);
        assertThat(cluster).host(2)
                .hasState(State.DOWN);
        assertThat(cluster).host(3)
                .hasState(State.UP)
                .isAtDistance(HostDistance.LOCAL);
        assertThat(session).hasPoolFor(3);

        // This is when refreshConnectedHost should have been invoked, it triggers pool creation when
        // we switch the node from IGNORED to UP:
        Mockito.verify(poolingOptions)
                .refreshConnectedHost(TestUtils.findHost(cluster, 3));
    }
}
