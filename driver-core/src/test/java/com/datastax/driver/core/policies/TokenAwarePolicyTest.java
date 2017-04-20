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

import com.datastax.driver.core.*;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT;
import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;

public class TokenAwarePolicyTest {

    /**
     * Ensures that {@link TokenAwarePolicy} will properly prioritize replicas if a provided
     * {@link SimpleStatement} is using an explicitly set keyspace and routing key and the
     * keyspace provided is using SimpleStrategy with a replication factor of 1.
     *
     * @test_category load_balancing:token_aware
     */
    @Test(groups = "short")
    public void should_choose_proper_host_based_on_routing_key() {
        // given: A 3 node cluster using TokenAwarePolicy with a replication factor of 1.
        ScassandraCluster sCluster = ScassandraCluster.builder()
                .withNodes(3)
                .withSimpleKeyspace("keyspace", 1)
                .build();
        Cluster cluster = Cluster.builder()
                .addContactPoints(sCluster.address(1).getAddress())
                .withPort(sCluster.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                .build();

        // when: A query is made with a routing key
        try {
            sCluster.init();

            Session session = cluster.connect();

            // Encodes into murmur hash '4557949199137838892' which should belong be owned by node 3.
            ByteBuffer routingKey = TypeCodec.varchar().serialize("should_choose_proper_host_based_on_routing_key", ProtocolVersion.NEWEST_SUPPORTED);
            SimpleStatement statement = new SimpleStatement("select * from table where k=5")
                    .setRoutingKey(routingKey)
                    .setKeyspace("keyspace");

            QueryTracker queryTracker = new QueryTracker();
            queryTracker.query(session, 10, statement);

            // then: The host having that token should be queried.
            queryTracker.assertQueried(sCluster, 1, 1, 0);
            queryTracker.assertQueried(sCluster, 1, 2, 0);
            queryTracker.assertQueried(sCluster, 1, 3, 10);
        } finally {
            cluster.close();
            sCluster.stop();
        }
    }

    /**
     * Ensures that {@link TokenAwarePolicy} will properly prioritize replicas in the local datacenter
     * if a provided {@link SimpleStatement} is using an explicitly set keyspace and routing key and
     * the keyspace provided is using NetworkTopologyStrategy with an RF of 1:1.
     *
     * @test_category load_balancing:token_aware
     */
    @Test(groups = "short")
    public void should_choose_host_in_local_dc_when_using_network_topology_strategy_and_dc_aware() {
        // given: A 6 node, 2 DC cluster with RF 1:1, using TokenAwarePolicy wrapping DCAwareRoundRobinPolicy with remote hosts.
        ScassandraCluster sCluster = ScassandraCluster.builder()
                .withNodes(3, 3)
                .withNetworkTopologyKeyspace("keyspace", ImmutableMap.of(1, 1, 2, 1))
                .build();
        Cluster cluster = Cluster.builder()
                .addContactPoints(sCluster.address(1).getAddress())
                .withPort(sCluster.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder()
                        .withLocalDc(ScassandraCluster.datacenter(2))
                        .withUsedHostsPerRemoteDc(3)
                        .build()))
                .build();

        // when: A query is made with a routing key
        try {
            sCluster.init();

            Session session = cluster.connect();

            // Encodes into murmur hash '-8124212968526248339' which should belong to 1:1 in DC1 and 2:1 in DC2.
            ByteBuffer routingKey = TypeCodec.varchar().serialize("should_choose_host_in_local_dc_when_using_network_topology_strategy_and_dc_aware", ProtocolVersion.NEWEST_SUPPORTED);
            SimpleStatement statement = new SimpleStatement("select * from table where k=5")
                    .setRoutingKey(routingKey)
                    .setKeyspace("keyspace");

            QueryTracker queryTracker = new QueryTracker();
            queryTracker.query(session, 10, statement);

            // then: The local replica (2:1) should be queried and never the remote one.
            queryTracker.assertQueried(sCluster, 2, 1, 10);
            queryTracker.assertQueried(sCluster, 1, 1, 0);
        } finally {
            cluster.close();
            sCluster.stop();
        }
    }

    /**
     * Ensures that {@link TokenAwarePolicy} will properly handle unavailability of replicas
     * matching with routing keys by falling back on its child policy and that when those
     * replicas become available the policy uses those replicas once again.
     *
     * @test_category load_balancing:token_aware
     */
    @Test(groups = "short")
    public void should_use_other_nodes_when_replicas_having_token_are_down() {
        // given: A 4 node cluster using TokenAwarePolicy with a replication factor of 2.
        ScassandraCluster sCluster = ScassandraCluster.builder()
                .withNodes(4)
                .withSimpleKeyspace("keyspace", 2)
                .build();
        Cluster cluster = Cluster.builder()
                .addContactPoints(sCluster.address(2).getAddress())
                .withPort(sCluster.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .withLoadBalancingPolicy(new TokenAwarePolicy(new SortingLoadBalancingPolicy()))
                .build();

        try {
            sCluster.init();

            Session session = cluster.connect();

            // when: A query is made with a routing key and both hosts having that key's token are down.
            // Encodes into murmur hash '6444339665561646341' which should belong to node 4.
            ByteBuffer routingKey = TypeCodec.varchar().serialize("should_use_other_nodes_when_replicas_having_token_are_down", ProtocolVersion.NEWEST_SUPPORTED);
            SimpleStatement statement = new SimpleStatement("select * from table where k=5")
                    .setRoutingKey(routingKey)
                    .setKeyspace("keyspace");

            QueryTracker queryTracker = new QueryTracker();
            queryTracker.query(session, 10, statement);

            // then: primary replica is 4, secondary is 1; since the child policy returns [1,2,3,4], the
            // TAP reorders the plan to [1,4,2,3]. Only 1 should be queried
            queryTracker.assertQueried(sCluster, 1, 1, 10);
            queryTracker.assertQueried(sCluster, 1, 2, 0);
            queryTracker.assertQueried(sCluster, 1, 3, 0);
            queryTracker.assertQueried(sCluster, 1, 4, 0);

            // when: The secondary node owning that key (1) goes down and a query is made.
            queryTracker.reset();
            sCluster.stop(cluster, 1);
            queryTracker.query(session, 10, statement);

            // then: The next replica having that data should be chosen (node 4 - primary replica).
            queryTracker.assertQueried(sCluster, 1, 1, 0);
            queryTracker.assertQueried(sCluster, 1, 2, 0);
            queryTracker.assertQueried(sCluster, 1, 3, 0);
            queryTracker.assertQueried(sCluster, 1, 4, 10);

            // when: All nodes having that token are down and a query is made.
            queryTracker.reset();
            sCluster.stop(cluster, 4);
            queryTracker.query(session, 10, statement);

            // then: The remaining nodes which are non-replicas of that token should be used
            // delegating to the child policy.
            queryTracker.assertQueried(sCluster, 1, 1, 0);
            queryTracker.assertQueried(sCluster, 1, 2, 10);
            queryTracker.assertQueried(sCluster, 1, 3, 0);
            queryTracker.assertQueried(sCluster, 1, 4, 0);

            // when: A replica having that key (4) becomes up and a query is made.
            queryTracker.reset();
            sCluster.start(cluster, 4);
            queryTracker.query(session, 10, statement);

            // then: The newly up replica should be queried.
            queryTracker.assertQueried(sCluster, 1, 1, 0);
            queryTracker.assertQueried(sCluster, 1, 2, 0);
            queryTracker.assertQueried(sCluster, 1, 3, 0);
            queryTracker.assertQueried(sCluster, 1, 4, 10);

            // when: The other replica becomes up and a query is made.
            queryTracker.reset();
            sCluster.start(cluster, 1);
            queryTracker.query(session, 10, statement);

            // then: The secondary replica (1) which is now up should be queried.
            queryTracker.assertQueried(sCluster, 1, 1, 10);
            queryTracker.assertQueried(sCluster, 1, 2, 0);
            queryTracker.assertQueried(sCluster, 1, 3, 0);
            queryTracker.assertQueried(sCluster, 1, 4, 0);
        } finally {
            cluster.close();
            sCluster.stop();
        }
    }

    /**
     * Validates that when overriding a routing key on a {@link BoundStatement}
     * using {@link BoundStatement#setRoutingKey(ByteBuffer...)} and
     * {@link BoundStatement#setRoutingKey(ByteBuffer)} that this routing key is used to determine
     * which hosts to route queries to.
     *
     * @test_category load_balancing:token_aware
     */
    @Test(groups = "short")
    public void should_use_provided_routing_key_boundstatement() {
        // given: A 4 node cluster using TokenAwarePolicy with a replication factor of 1.
        ScassandraCluster sCluster = ScassandraCluster.builder()
                .withNodes(4)
                .withSimpleKeyspace("keyspace", 1)
                .build();
        Cluster cluster = Cluster.builder()
                .addContactPoints(sCluster.address(2).getAddress())
                .withPort(sCluster.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                .build();

        try {
            sCluster.init();

            Session session = cluster.connect("keyspace");

            PreparedStatement preparedStatement = session.prepare("insert into tbl (k0, v) values (?, ?)");
            // bind text values since scassandra defaults to use varchar if not primed.
            // this is inconsequential in this case since we are explicitly providing the routing key.
            BoundStatement bs = preparedStatement.bind("a", "b");

            // Derive a routing key for single routing key component, this should resolve to
            // '4891967783720036163'
            ByteBuffer routingKey = TypeCodec.bigint().serialize(33L, ProtocolVersion.NEWEST_SUPPORTED);
            bs.setRoutingKey(routingKey);

            QueryTracker queryTracker = new QueryTracker();
            queryTracker.query(session, 10, bs);

            // Expect only node 3 to have been queried, give it has ownership of that partition
            // (token range is (4611686018427387902, 6917529027641081853])
            queryTracker.assertQueried(sCluster, 1, 1, 0);
            queryTracker.assertQueried(sCluster, 1, 2, 0);
            queryTracker.assertQueried(sCluster, 1, 3, 0);
            queryTracker.assertQueried(sCluster, 1, 4, 10);

            // reset counts.
            queryTracker.reset();

            // Derive a routing key for multiple routing key components, this should resolve to
            // '3735658072872431718'
            bs = preparedStatement.bind("a", "b");
            ByteBuffer routingKeyK0Part = TypeCodec.bigint().serialize(42L, ProtocolVersion.NEWEST_SUPPORTED);
            ByteBuffer routingKeyK1Part = TypeCodec.varchar().serialize("hello_world", ProtocolVersion.NEWEST_SUPPORTED);
            bs.setRoutingKey(routingKeyK0Part, routingKeyK1Part);

            queryTracker.query(session, 10, bs);

            // Expect only node 3 to have been queried, give it has ownership of that partition
            // (token range is (2305843009213693951, 4611686018427387902])
            queryTracker.assertQueried(sCluster, 1, 1, 0);
            queryTracker.assertQueried(sCluster, 1, 2, 0);
            queryTracker.assertQueried(sCluster, 1, 3, 10);
            queryTracker.assertQueried(sCluster, 1, 4, 0);
        } finally {
            cluster.close();
            sCluster.stop();
        }
    }

    /**
     * Ensures that {@link TokenAwarePolicy} will properly handle a routing key for a {@link PreparedStatement}
     * whose table uses multiple columns for its partition key.
     *
     * @test_category load_balancing:token_aware
     * @jira_ticket JAVA-123 (to ensure routing key buffers are not destroyed).
     */
    @Test(groups = "long")
    public void should_properly_generate_and_use_routing_key_for_composite_partition_key() {

        // given: a 3 node cluster with a keyspace with RF 1.
        CCMBridge ccm = CCMBridge.builder()
                .withNodes(3)
                .build();

        ccm.start();

        Cluster cluster = Cluster.builder()
                .addContactPoints(ccm.addressOfNode(1).getAddress())
                .withPort(ccm.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                .build();

        try {

            Session session = cluster.connect();

            String ks = TestUtils.generateIdentifier("ks_");
            session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, ks, 1));
            session.execute("USE " + ks);
            session.execute("CREATE TABLE composite (k1 int, k2 int, i int, PRIMARY KEY ((k1, k2)))");

            // (0,1) resolves to token '-5343711339996600080' which belongs to node 2 so all queries should go to that node.
            PreparedStatement insertPs = session.prepare("INSERT INTO composite(k1, k2, i) VALUES (?, ?, ?)");
            BoundStatement insertBs = insertPs.bind(0, 1, 2);

            PreparedStatement selectPs = session.prepare("SELECT * FROM composite WHERE k1=? and k2=?");
            BoundStatement selectBs = selectPs.bind(0, 1);

            // when: executing a prepared statement with a composite partition key.
            // then: should query the correct node (2) in for both insert and select queries.
            for (int i = 0; i < 10; i++) {
                ResultSet rs = session.execute(insertBs);
                assertThat(rs.getExecutionInfo().getQueriedHost().getSocketAddress()).isEqualTo(ccm.addressOfNode(2));

                rs = session.execute(selectBs);
                assertThat(rs.getExecutionInfo().getQueriedHost().getSocketAddress()).isEqualTo(ccm.addressOfNode(2));
                assertThat(rs.isExhausted()).isFalse();
                Row r = rs.one();
                assertThat(rs.isExhausted()).isTrue();

                assertThat(r.getInt("i")).isEqualTo(2);
            }
        } finally {
            cluster.close();
            ccm.remove();
        }
    }

}
