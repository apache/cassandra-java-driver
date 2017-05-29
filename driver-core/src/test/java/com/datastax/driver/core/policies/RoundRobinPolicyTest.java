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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;

public class RoundRobinPolicyTest {

    Logger policyLogger = Logger.getLogger(RoundRobinPolicy.class);
    MemoryAppender logs;
    QueryTracker queryTracker;
    Level originalLevel;

    @BeforeMethod(groups = "short")
    public void setUp() {
        queryTracker = new QueryTracker();
        originalLevel = policyLogger.getLevel();
        policyLogger.setLevel(Level.WARN);
        logs = new MemoryAppender();
        policyLogger.addAppender(logs);
    }

    @AfterMethod(groups = "short", alwaysRun = true)
    public void tearDown() {
        policyLogger.setLevel(originalLevel);
        policyLogger.removeAppender(logs);
    }

    /**
     * Ensures that when used {@link RoundRobinPolicy} properly round robins requests within
     * nodes in a single datacenter.
     *
     * @test_category load_balancing:round_robin
     */
    @Test(groups = "short")
    public void should_round_robin_within_single_datacenter() {
        // given: a 5 node cluster using RoundRobinPolicy.
        ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(5).build();

        Cluster cluster = Cluster.builder()
                .addContactPoints(sCluster.address(1).getAddress())
                .withPort(sCluster.getBinaryPort())
                .withLoadBalancingPolicy(new RoundRobinPolicy())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        try {
            sCluster.init();

            Session session = cluster.connect();
            // when: a query is executed 50 times.
            queryTracker.query(session, 50);

            // then: all nodes should be queried equally.
            for (int i = 1; i <= 5; i++) {
                queryTracker.assertQueried(sCluster, 1, i, 10);
            }
        } finally {
            cluster.close();
            sCluster.stop();
        }

    }

    /**
     * Ensures that when used {@link RoundRobinPolicy} properly round robins requests to nodes irrespective
     * of cluster topology by ensuring nodes in different data centers are queried equally to others.
     *
     * @test_category load_balancing:round_robin
     */
    @Test(groups = "short")
    public void should_round_robin_irrespective_of_topology() {
        // given: a 10 node, 5 DC cluster using RoundRobinPolicy.
        ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(2, 2, 2, 2, 2).build();

        Cluster cluster = Cluster.builder()
                .addContactPoints(sCluster.address(1).getAddress())
                .withPort(sCluster.getBinaryPort())
                .withLoadBalancingPolicy(new RoundRobinPolicy())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        try {
            sCluster.init();

            Session session = cluster.connect();
            // when: a query is executed 50 times.
            queryTracker.query(session, 50);

            // then: all nodes should be queried equally.
            for (int dc = 1; dc <= 5; dc++) {
                for (int i = 1; i <= 2; i++) {
                    queryTracker.assertQueried(sCluster, dc, i, 5);
                }
            }
        } finally {
            cluster.close();
            sCluster.stop();
        }
    }

    /**
     * Ensures that when used {@link RoundRobinPolicy} generates a warning if a consistency level is used
     * that is data center local (i.e. LOCAL_QUORUM) and nodes from multiple data centers are in the cluster
     * and that warning is only generated once.   Also validates that is a non-Dc local consistency level is
     * used (i.e. ONE) that no such warning is generated.
     *
     * @test_category load_balancing:round_robin
     */
    @Test(groups = "short", dataProvider = "consistencyLevels", dataProviderClass = DataProviders.class)
    public void should_warn_if_using_dc_local_consistency_level(ConsistencyLevel cl) {
        // given: a 2 node, 2 DC cluster using RoundRobinPolicy.
        ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(1, 1).build();

        Cluster cluster = Cluster.builder()
                .addContactPoints(sCluster.address(1).getAddress())
                .withPort(sCluster.getBinaryPort())
                .withLoadBalancingPolicy(new RoundRobinPolicy())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        String expectedLogMessage = "Detected request at Consistency Level " + cl + " but the non-DC aware RoundRobinPolicy is in use.";

        try {
            sCluster.init();

            Session session = cluster.connect();
            // when: a query is executed 50 times.
            queryTracker.query(session, 50, cl);

            // then: all nodes should be queried equally.
            queryTracker.assertQueried(sCluster, 1, 1, 25);
            queryTracker.assertQueried(sCluster, 2, 1, 25);

            // Should get a warning if using a local DC cl.
            if (cl.isDCLocal()) {
                assertThat(logs.get()).containsOnlyOnce(expectedLogMessage);
            } else {
                assertThat(logs.get()).doesNotContain(expectedLogMessage);
            }
        } finally {
            cluster.close();
            sCluster.stop();
        }
    }
}
