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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static com.datastax.driver.core.TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT;
import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;

@CreateCCM(PER_METHOD)
@CCMConfig(createCcm = false)
public class TokenAwarePolicyTest extends CCMTestsSupport {

    QueryTracker queryTracker;

    @BeforeMethod(groups = "short")
    public void setUp() {
        queryTracker = new QueryTracker();
    }

    @DataProvider(name = "shuffleProvider")
    public Object[][] shuffleProvider() {
        return new Object[][]{
                {true},
                {false},
                {null}
        };
    }

    /**
     * Ensures that {@link TokenAwarePolicy} will shuffle discovered replicas depending on the value of shuffleReplicas
     * used when constructing with {@link TokenAwarePolicy#TokenAwarePolicy(LoadBalancingPolicy, boolean)} and that if not
     * provided replicas are shuffled by default when using {@link TokenAwarePolicy#TokenAwarePolicy(LoadBalancingPolicy, boolean)}.
     *
     * @test_category load_balancing:token_aware
     */
    @Test(groups = "short", dataProvider = "shuffleProvider")
    public void should_shuffle_replicas_based_on_configuration(Boolean shuffleReplicas) {
        // given: an 8 node cluster using TokenAwarePolicy and some shuffle replica configuration with a keyspace with replication factor of 3.
        ScassandraCluster sCluster = ScassandraCluster.builder()
                .withNodes(8)
                .withSimpleKeyspace("keyspace", 3)
                .build();

        LoadBalancingPolicy loadBalancingPolicy;
        if (shuffleReplicas == null) {
            loadBalancingPolicy = new TokenAwarePolicy(new RoundRobinPolicy());
            shuffleReplicas = true;
        } else {
            loadBalancingPolicy = new TokenAwarePolicy(new RoundRobinPolicy(), shuffleReplicas);
        }

        Cluster cluster = Cluster.builder()
                .addContactPoints(sCluster.address(1).getAddress())
                .withPort(sCluster.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .build();

        try {
            sCluster.init();

            Session session = cluster.connect();

            // given: A routing key that falls in the token range of node 6.

            // Encodes into murmur hash '4874351301193663061' which should belong be owned by node 6 with replicas 7 and 8.
            ByteBuffer routingKey = TypeCodec.varchar().serialize("This is some sample text", ProtocolVersion.NEWEST_SUPPORTED);

            // then: The replicas resolved from the cluster metadata must match node 6 and its replicas.
            List<Host> replicas = Lists.newArrayList(cluster.getMetadata().getReplicas("keyspace", routingKey));
            assertThat(replicas).containsExactly(
                    sCluster.host(cluster, 1, 6),
                    sCluster.host(cluster, 1, 7),
                    sCluster.host(cluster, 1, 8));

            // then: generating a query plan on a statement using that routing key should properly prioritize node 6 and its replicas.
            // Actual query does not matter, only the keyspace and routing key will be used
            SimpleStatement statement = new SimpleStatement("select * from table where k=5");
            statement.setRoutingKey(routingKey);
            statement.setKeyspace("keyspace");

            boolean shuffledAtLeastOnce = false;
            for (int i = 0; i < 1024; i++) {
                List<Host> queryPlan = Lists.newArrayList(loadBalancingPolicy.newQueryPlan(null, statement));
                assertThat(queryPlan).containsOnlyElementsOf(cluster.getMetadata().getAllHosts());

                List<Host> firstThree = queryPlan.subList(0, 3);
                // then: if shuffle replicas was used or using default, the first three hosts returned should be 6,7,8 in any order.
                //       if shuffle replicas was not used, the first three hosts returned should be 6,7,8 in that order.
                if (shuffleReplicas) {
                    assertThat(firstThree).containsOnlyElementsOf(replicas);
                    if (!firstThree.equals(replicas)) {
                        shuffledAtLeastOnce = true;
                    }
                } else {
                    assertThat(firstThree).containsExactlyElementsOf(replicas);
                }
            }

            // then: given 1024 query plans, the replicas should be shuffled at least once.
            assertThat(shuffledAtLeastOnce).isEqualTo(shuffleReplicas);
        } finally {
            cluster.close();
            sCluster.stop();
        }
    }

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
                // Don't shuffle replicas just to keep test deterministic.
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy(), false))
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

            queryTracker.query(session, 10, statement);

            // then: The node that is the primary for that key's hash is chosen.
            queryTracker.assertQueried(sCluster, 1, 1, 0);
            queryTracker.assertQueried(sCluster, 1, 2, 0);
            queryTracker.assertQueried(sCluster, 1, 3, 0);
            queryTracker.assertQueried(sCluster, 1, 4, 10);

            // when: The primary node owning that key goes down and a query is made.
            queryTracker.reset();
            sCluster.stop(cluster, 4);
            queryTracker.query(session, 10, statement);

            // then: The next replica having that data should be chosen (node 1).
            queryTracker.assertQueried(sCluster, 1, 1, 10);
            queryTracker.assertQueried(sCluster, 1, 2, 0);
            queryTracker.assertQueried(sCluster, 1, 3, 0);
            queryTracker.assertQueried(sCluster, 1, 4, 0);

            // when: All nodes having that token are down and a query is made.
            queryTracker.reset();
            sCluster.stop(cluster, 1);
            queryTracker.query(session, 10, statement);

            // then: The remaining nodes which are non-replicas of that token should be used
            // delegating to the child policy (RoundRobin).
            queryTracker.assertQueried(sCluster, 1, 1, 0);
            queryTracker.assertQueried(sCluster, 1, 2, 5);
            queryTracker.assertQueried(sCluster, 1, 3, 5);
            queryTracker.assertQueried(sCluster, 1, 4, 0);

            // when: A replica having that key becomes up and a query is made.
            queryTracker.reset();
            sCluster.start(cluster, 1);
            queryTracker.query(session, 10, statement);

            // then: The newly up replica should be queried.
            queryTracker.assertQueried(sCluster, 1, 1, 10);
            queryTracker.assertQueried(sCluster, 1, 2, 0);
            queryTracker.assertQueried(sCluster, 1, 3, 0);
            queryTracker.assertQueried(sCluster, 1, 4, 0);

            // when: The primary replicas becomes up and a query is made.
            queryTracker.reset();
            sCluster.start(cluster, 4);
            queryTracker.query(session, 10, statement);

            // then: The primary replica which is now up should be queried.
            queryTracker.assertQueried(sCluster, 1, 1, 0);
            queryTracker.assertQueried(sCluster, 1, 2, 0);
            queryTracker.assertQueried(sCluster, 1, 3, 0);
            queryTracker.assertQueried(sCluster, 1, 4, 10);
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
                // Don't shuffle replicas just to keep test deterministic.
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy(), false))
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
    @CCMConfig(createCcm = true, numberOfNodes = 3, createCluster = false)
    @Test(groups = "long")
    public void should_properly_generate_and_use_routing_key_for_composite_partition_key() {
        // given: a 3 node cluster with a keyspace with RF 1.
        Cluster cluster = register(Cluster.builder()
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .build());
        Session session = cluster.connect();

        String table = "composite";
        String ks = TestUtils.generateIdentifier("ks_");
        session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, ks, 1));
        session.execute("USE " + ks);
        session.execute(String.format("CREATE TABLE %s (k1 int, k2 int, i int, PRIMARY KEY ((k1, k2)))", table));

        // (1,2) resolves to token '4881097376275569167' which belongs to node 1 so all queries should go to that node.
        PreparedStatement insertPs = session.prepare("INSERT INTO " + table + "(k1, k2, i) VALUES (?, ?, ?)");
        BoundStatement insertBs = insertPs.bind(1, 2, 3);

        PreparedStatement selectPs = session.prepare("SELECT * FROM " + table + " WHERE k1=? and k2=?");
        BoundStatement selectBs = selectPs.bind(1, 2);

        // when: executing a prepared statement with a composite partition key.
        // then: should query the correct node (1) in for both insert and select queries.
        for (int i = 0; i < 10; i++) {
            ResultSet rs = session.execute(insertBs);
            assertThat(rs.getExecutionInfo().getQueriedHost()).isEqualTo(TestUtils.findHost(cluster, 1));

            rs = session.execute(selectBs);
            assertThat(rs.getExecutionInfo().getQueriedHost()).isEqualTo(TestUtils.findHost(cluster, 1));
            assertThat(rs.isExhausted()).isFalse();
            Row r = rs.one();
            assertThat(rs.isExhausted()).isTrue();

            assertThat(r.getInt("i")).isEqualTo(3);
        }
    }
}
