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

import java.util.Set;

import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;

public class SingleTokenIntegrationTest {
    /**
     * JAVA-684: Empty TokenRange returned in a one token cluster
     */
    @Test(groups = "unit")
    public void should_return_single_non_empty_range_when_cluster_has_one_single_token() {
        CCMBridge ccm = null;
        Cluster cluster = null;

        try {
            ccm = CCMBridge.create("test");
            // Set an initial token that is not the minimum
            ccm.updateConfig("initial_token", "1");
            ccm.bootstrapNode(1);
            ccm.start();

            cluster = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1)).build();

            Session session = cluster.connect();
            session.execute("create keyspace test with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

            Set<TokenRange> tokenRanges = cluster.getMetadata().getTokenRanges();
            assertThat(tokenRanges).hasSize(1);
            TokenRange tokenRange = tokenRanges.iterator().next();
            assertThat(tokenRange)
                .startsWith(Token.M3PToken.FACTORY.minToken())
                .endsWith(Token.M3PToken.FACTORY.minToken())
                .isNotEmpty()
                .isNotWrappedAround();

            Set<Host> rangeHosts = cluster.getMetadata().getReplicas("test", tokenRange);
            assertThat(rangeHosts)
                .containsOnly(TestUtils.findHost(cluster, 1));

        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }
}
