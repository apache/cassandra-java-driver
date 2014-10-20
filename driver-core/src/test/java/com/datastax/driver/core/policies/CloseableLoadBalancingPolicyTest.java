package com.datastax.driver.core.policies;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.Cluster;

public class CloseableLoadBalancingPolicyTest {
    @Test(groups = "short")
    public void should_be_invoked_at_shutdown() {
        CloseMonitoringPolicy policy = new CloseMonitoringPolicy(Policies.defaultLoadBalancingPolicy());
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            ccm = CCMBridge.create("test", 1);
            cluster = Cluster.builder()
                             .addContactPoint(CCMBridge.ipOfNode(1))
                             .withLoadBalancingPolicy(policy)
                             .build();
            cluster.connect();
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
        assertThat(policy.wasClosed).isTrue();
    }

    static class CloseMonitoringPolicy extends DelegatingLoadBalancingPolicy {

        volatile boolean wasClosed = false;

        public CloseMonitoringPolicy(LoadBalancingPolicy delegate) {
            super(delegate);
        }

        @Override
        public void close() {
            wasClosed = true;
        }
    }
}
