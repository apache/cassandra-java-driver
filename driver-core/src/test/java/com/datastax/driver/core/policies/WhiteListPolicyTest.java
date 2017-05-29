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

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.collect.Lists;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;

public class WhiteListPolicyTest {

    QueryTracker queryTracker;

    @BeforeMethod(groups = "short")
    public void setUp() {
        queryTracker = new QueryTracker();
    }

    /**
     * Provides basic validation of {@link WhiteListPolicy}.
     * <p/>
     * Ensures that:
     * <ol>
     * <li>Only addresses provided in the whitelist are every used for querying.</li>
     * <li>If no nodes present in the whitelist are available, queries fail with a {@link NoHostAvailableException}</li>
     * </ol>
     *
     * @test_category load_balancing:white_list
     */
    @Test(groups = "short")
    public void should_only_query_hosts_in_white_list() throws Exception {
        // given: a 5 node cluster with a WhiteListPolicy targeting nodes 3 and 5.
        ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(5).build();
        List<InetSocketAddress> whiteList = Lists.newArrayList(sCluster.address(3), sCluster.address(5));

        Cluster cluster = Cluster.builder()
                .addContactPoints(sCluster.address(5).getAddress())
                .withPort(sCluster.getBinaryPort())
                .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(), whiteList))
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        try {
            sCluster.init();

            Session session = cluster.connect();
            // when: a query is executed 50 times.
            queryTracker.query(session, 50);

            // then: only nodes 3 and 5 should have been queried.
            queryTracker.assertQueried(sCluster, 1, 1, 0);
            queryTracker.assertQueried(sCluster, 1, 2, 0);
            queryTracker.assertQueried(sCluster, 1, 3, 25);
            queryTracker.assertQueried(sCluster, 1, 4, 0);
            queryTracker.assertQueried(sCluster, 1, 5, 25);

            queryTracker.reset();

            // when: the only nodes in the whitelist are stopped.
            sCluster.stop(cluster, 3);
            sCluster.stop(cluster, 5);

            // then: all queries should raise a NHAE.
            queryTracker.query(session, 50, ConsistencyLevel.ONE, NoHostAvailableException.class);

            queryTracker.assertQueried(sCluster, 1, 1, 0);
            queryTracker.assertQueried(sCluster, 1, 2, 0);
            queryTracker.assertQueried(sCluster, 1, 3, 0);
            queryTracker.assertQueried(sCluster, 1, 4, 0);
            queryTracker.assertQueried(sCluster, 1, 5, 0);
        } finally {
            cluster.close();
            sCluster.stop();
        }
    }

    /**
     * Validates that a {@link Cluster} cannot be initiated if using a {@link WhiteListPolicy} and
     * none of the specified contact point addresses are present in the white list.
     *
     * @test_category load_balancing:white_list
     */
    @Test(groups = "short", expectedExceptions = {IllegalArgumentException.class})
    public void should_require_contact_point_in_white_list() throws Exception {
        // given: a 5 node cluster with a WhiteListPolicy targeting node2.
        ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(5).build();

        // when: using a Cluster instance with none of the contact points in the declared
        // WhiteListPolicy.
        List<InetSocketAddress> whiteList = Lists.newArrayList(sCluster.address(2));
        Cluster cluster = Cluster.builder()
                .addContactPointsWithPorts(sCluster.address(3))
                .withPort(sCluster.getBinaryPort())
                .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(), whiteList))
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        try {
            sCluster.init();
            // then: The cluster instance should fail to initialize as none of the contact
            // points is present in the white list.
            cluster.init();
        } finally {
            cluster.close();
            sCluster.stop();
        }
    }
}
