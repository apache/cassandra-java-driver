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
            ccm = CCMBridge.builder("test").withNodes(1).build();
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
