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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.CCMConfig;
import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.Cluster;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

@CCMConfig(createSession = false)
public class CloseableLoadBalancingPolicyTest extends CCMTestsSupport {

    private CloseMonitoringPolicy policy;

    @Test(groups = "short")
    public void should_be_invoked_at_shutdown() {
        try {
            cluster().connect();
            cluster().close();
        } finally {
            assertThat(policy.wasClosed).isTrue();
        }
    }

    @Override
    public Cluster.Builder createClusterBuilder() {
        policy = new CloseMonitoringPolicy(Policies.defaultLoadBalancingPolicy());
        return Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withLoadBalancingPolicy(policy);
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
