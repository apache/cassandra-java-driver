package com.datastax.driver.core;

import static java.util.concurrent.TimeUnit.SECONDS;

import org.mockito.Mockito;
import org.testng.annotations.Test;

import com.datastax.driver.core.Host.State;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.LimitingLoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;

import static com.datastax.driver.core.Assertions.assertThat;

public class PoolingOptionsTest {

    /**
     * Tests {@link PoolingOptions#refreshConnectedHost(Host)} through a custom load balancing policy.
     */
    @Test(groups = "long")
    public void should_refresh_single_connected_host() {
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            // This will make the driver use at most 2 hosts, the others will be ignored
            LimitingLoadBalancingPolicy loadBalancingPolicy = new LimitingLoadBalancingPolicy(new RoundRobinPolicy(), 2, 1);

            // Setup a 3-host cluster, start only two hosts so that we know in advance which ones the policy will use
            ccm = CCMBridge.create("test");
            ccm.populate(3);
            ccm.start(1);
            ccm.start(2);
            ccm.waitForUp(1);
            ccm.waitForUp(2);

            PoolingOptions poolingOptions = Mockito.spy(new PoolingOptions());
            cluster = Cluster.builder()
                             .addContactPoint(CCMBridge.ipOfNode(1))
                             .withPoolingOptions(poolingOptions)
                             .withLoadBalancingPolicy(loadBalancingPolicy)
                             .withReconnectionPolicy(new ConstantReconnectionPolicy(1000))
                             .build();

            Session session = cluster.connect();

            assertThat(cluster).usesControlHost(1);
            assertThat(cluster).host(1)
                               .hasState(State.UP)
                               .isAtDistance(HostDistance.LOCAL);
            // Wait for the node to be up, because apparently on Jenkins it's still only ADDED when we reach this line
            assertThat(cluster).host(2)
                               .comesUpWithin(120, SECONDS)
                               .isAtDistance(HostDistance.LOCAL);

            // Bring host 3 up, its presence should be acknowledged but it should be ignored
            ccm.start(3);
            ccm.waitForUp(3);

            assertThat(cluster).host(1)
                               .hasState(State.UP)
                               .isAtDistance(HostDistance.LOCAL);
            assertThat(cluster).host(2)
                               .hasState(State.UP)
                               .isAtDistance(HostDistance.LOCAL);
            assertThat(cluster).host(3)
                               .hasState(State.UP)
                               .isAtDistance(HostDistance.IGNORED);
            assertThat(session).hasNoPoolFor(3);

            // Kill host 2, host 3 should take its place
            ccm.stop(2);
            TestUtils.waitFor(CCMBridge.ipOfNode(3), cluster, 120);

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

        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }
}
