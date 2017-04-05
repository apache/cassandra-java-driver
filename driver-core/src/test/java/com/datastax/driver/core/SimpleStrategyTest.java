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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SimpleStrategyTest extends AbstractReplicationStrategyTest {

    private static ReplicationStrategy simpleStrategy(int replicationFactor) {
        return ReplicationStrategy.create(ImmutableMap.<String, String>builder()//
                .put("class", "SimpleStrategy")//
                .put("replication_factor", String.valueOf(replicationFactor))//
                .build());
    }

    /*
     * ---------------------------------------------------------------------------
     * Ring, replication, etc... setup. These are reusable for the tests
     * This data is based on a real ring topology. Some tests are using
     * smaller and more specific topologies instead.
     * ---------------------------------------------------------------------------
     */

    private static final Token TOKEN01 = token("-9000000000000000000");
    private static final Token TOKEN02 = token("-8000000000000000000");
    private static final Token TOKEN03 = token("-7000000000000000000");
    private static final Token TOKEN04 = token("-6000000000000000000");
    private static final Token TOKEN05 = token("-5000000000000000000");
    private static final Token TOKEN06 = token("-4000000000000000000");
    private static final Token TOKEN07 = token("-3000000000000000000");
    private static final Token TOKEN08 = token("-2000000000000000000");
    private static final Token TOKEN09 = token("-1000000000000000000");
    private static final Token TOKEN10 = token("0");
    private static final Token TOKEN11 = token("1000000000000000000");
    private static final Token TOKEN12 = token("2000000000000000000");
    private static final Token TOKEN13 = token("3000000000000000000");
    private static final Token TOKEN14 = token("4000000000000000000");
    private static final Token TOKEN15 = token("5000000000000000000");
    private static final Token TOKEN16 = token("6000000000000000000");
    private static final Token TOKEN17 = token("7000000000000000000");
    private static final Token TOKEN18 = token("8000000000000000000");
    private static final Token TOKEN19 = token("9000000000000000000");

    private static final InetSocketAddress IP1 = socketAddress("127.0.0.101");
    private static final InetSocketAddress IP2 = socketAddress("127.0.0.102");
    private static final InetSocketAddress IP3 = socketAddress("127.0.0.103");
    private static final InetSocketAddress IP4 = socketAddress("127.0.0.104");
    private static final InetSocketAddress IP5 = socketAddress("127.0.0.105");
    private static final InetSocketAddress IP6 = socketAddress("127.0.0.106");

    private static final ReplicationStrategy exampleStrategy = simpleStrategy(3);

    private static final ReplicationStrategy exampleStrategyTooManyReplicas = simpleStrategy(8);

    private static final List<Token> exampleRing = ImmutableList.<Token>builder()
            .add(TOKEN01)
            .add(TOKEN02)
            .add(TOKEN03)
            .add(TOKEN04)
            .add(TOKEN05)
            .add(TOKEN06)
            .add(TOKEN07)
            .add(TOKEN08)
            .add(TOKEN09)
            .add(TOKEN10)
            .add(TOKEN11)
            .add(TOKEN12)
            .add(TOKEN13)
            .add(TOKEN14)
            .add(TOKEN15)
            .add(TOKEN16)
            .add(TOKEN17)
            .add(TOKEN18)
            .build();

    private static final Map<Token, Host> exampleTokenToPrimary = ImmutableMap.<Token, Host>builder()
            .put(TOKEN01, host(IP1))
            .put(TOKEN02, host(IP1))
            .put(TOKEN03, host(IP5))
            .put(TOKEN04, host(IP3))
            .put(TOKEN05, host(IP1))
            .put(TOKEN06, host(IP5))
            .put(TOKEN07, host(IP2))
            .put(TOKEN08, host(IP6))
            .put(TOKEN09, host(IP3))
            .put(TOKEN10, host(IP4))
            .put(TOKEN11, host(IP5))
            .put(TOKEN12, host(IP4))
            .put(TOKEN13, host(IP4))
            .put(TOKEN14, host(IP2))
            .put(TOKEN15, host(IP6))
            .put(TOKEN16, host(IP3))
            .put(TOKEN17, host(IP2))
            .put(TOKEN18, host(IP6))
            .build();

    private static final String keyspace = "excalibur";

    /*
     * --------------
     *     Tests
     * --------------
     */

    @Test(groups = "unit")
    public void simpleStrategySimpleTopologyTest() {
        List<Token> ring = ImmutableList.<Token>builder()
                .add(TOKEN01)
                .add(TOKEN06)
                .add(TOKEN14)
                .add(TOKEN19)
                .build();

        Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
                .put(TOKEN01, host(IP1))
                .put(TOKEN06, host(IP2))
                .put(TOKEN14, host(IP1))
                .put(TOKEN19, host(IP2))
                .build();

        ReplicationStrategy strategy = simpleStrategy(2);

        Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(keyspace, tokenToPrimary, ring);

        assertReplicaPlacement(replicaMap, TOKEN01, IP1, IP2);
        assertReplicaPlacement(replicaMap, TOKEN06, IP2, IP1);
        assertReplicaPlacement(replicaMap, TOKEN14, IP1, IP2);
        assertReplicaPlacement(replicaMap, TOKEN19, IP2, IP1);
    }

    @Test(groups = "unit")
    public void simpleStrategyConsecutiveRingSectionsTest() {
        List<Token> ring = ImmutableList.<Token>builder()
                .add(TOKEN01)
                .add(TOKEN06)
                .add(TOKEN14)
                .add(TOKEN19)
                .build();

        Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
                .put(TOKEN01, host(IP1))
                .put(TOKEN06, host(IP1))
                .put(TOKEN14, host(IP2))
                .put(TOKEN19, host(IP2))
                .build();

        ReplicationStrategy strategy = simpleStrategy(2);

        Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(keyspace, tokenToPrimary, ring);

        assertReplicaPlacement(replicaMap, TOKEN01, IP1, IP2);
        assertReplicaPlacement(replicaMap, TOKEN06, IP1, IP2);
        assertReplicaPlacement(replicaMap, TOKEN14, IP2, IP1);
        assertReplicaPlacement(replicaMap, TOKEN19, IP2, IP1);
    }

    @Test(groups = "unit")
    public void simpleStrategyUnbalancedRingTest() {
        List<Token> ring = ImmutableList.<Token>builder()
                .add(TOKEN01)
                .add(TOKEN06)
                .add(TOKEN14)
                .add(TOKEN19)
                .build();

        Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
                .put(TOKEN01, host(IP1))
                .put(TOKEN06, host(IP1))
                .put(TOKEN14, host(IP2))
                .put(TOKEN19, host(IP1))
                .build();

        ReplicationStrategy strategy = simpleStrategy(2);

        Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(keyspace, tokenToPrimary, ring);

        assertReplicaPlacement(replicaMap, TOKEN01, IP1, IP2);
        assertReplicaPlacement(replicaMap, TOKEN06, IP1, IP2);
        assertReplicaPlacement(replicaMap, TOKEN14, IP2, IP1);
        assertReplicaPlacement(replicaMap, TOKEN19, IP1, IP2);
    }

    @Test(groups = "unit")
    public void simpleStrategyExampleTopologyMapTest() {
        Map<Token, Set<Host>> replicaMap = exampleStrategy.computeTokenToReplicaMap(keyspace, exampleTokenToPrimary, exampleRing);

        assertReplicaPlacement(replicaMap, TOKEN01, IP1, IP5, IP3);
        assertReplicaPlacement(replicaMap, TOKEN02, IP1, IP5, IP3);
        assertReplicaPlacement(replicaMap, TOKEN03, IP5, IP3, IP1);
        assertReplicaPlacement(replicaMap, TOKEN04, IP3, IP1, IP5);
        assertReplicaPlacement(replicaMap, TOKEN05, IP1, IP5, IP2);
        assertReplicaPlacement(replicaMap, TOKEN06, IP5, IP2, IP6);
        assertReplicaPlacement(replicaMap, TOKEN07, IP2, IP6, IP3);
        assertReplicaPlacement(replicaMap, TOKEN08, IP6, IP3, IP4);
        assertReplicaPlacement(replicaMap, TOKEN09, IP3, IP4, IP5);
        assertReplicaPlacement(replicaMap, TOKEN10, IP4, IP5, IP2);
        assertReplicaPlacement(replicaMap, TOKEN11, IP5, IP4, IP2);
        assertReplicaPlacement(replicaMap, TOKEN12, IP4, IP2, IP6);
        assertReplicaPlacement(replicaMap, TOKEN13, IP4, IP2, IP6);
        assertReplicaPlacement(replicaMap, TOKEN14, IP2, IP6, IP3);
        assertReplicaPlacement(replicaMap, TOKEN15, IP6, IP3, IP2);
        assertReplicaPlacement(replicaMap, TOKEN16, IP3, IP2, IP6);
        assertReplicaPlacement(replicaMap, TOKEN17, IP2, IP6, IP1);
        assertReplicaPlacement(replicaMap, TOKEN18, IP6, IP1, IP5);
    }

    @Test(groups = "unit")
    public void simpleStrategyExampleTopologyTooManyReplicasTest() {
        Map<Token, Set<Host>> replicaMap = exampleStrategyTooManyReplicas.computeTokenToReplicaMap(keyspace, exampleTokenToPrimary, exampleRing);

        assertReplicaPlacement(replicaMap, TOKEN01, IP1, IP5, IP3, IP2, IP6, IP4);
        assertReplicaPlacement(replicaMap, TOKEN02, IP1, IP5, IP3, IP2, IP6, IP4);
        assertReplicaPlacement(replicaMap, TOKEN03, IP5, IP3, IP1, IP2, IP6, IP4);
        assertReplicaPlacement(replicaMap, TOKEN04, IP3, IP1, IP5, IP2, IP6, IP4);
        assertReplicaPlacement(replicaMap, TOKEN05, IP1, IP5, IP2, IP6, IP3, IP4);
        assertReplicaPlacement(replicaMap, TOKEN06, IP5, IP2, IP6, IP3, IP4, IP1);
        assertReplicaPlacement(replicaMap, TOKEN07, IP2, IP6, IP3, IP4, IP5, IP1);
        assertReplicaPlacement(replicaMap, TOKEN08, IP6, IP3, IP4, IP5, IP2, IP1);
        assertReplicaPlacement(replicaMap, TOKEN09, IP3, IP4, IP5, IP2, IP6, IP1);
        assertReplicaPlacement(replicaMap, TOKEN10, IP4, IP5, IP2, IP6, IP3, IP1);
        assertReplicaPlacement(replicaMap, TOKEN11, IP5, IP4, IP2, IP6, IP3, IP1);
        assertReplicaPlacement(replicaMap, TOKEN12, IP4, IP2, IP6, IP3, IP1, IP5);
        assertReplicaPlacement(replicaMap, TOKEN13, IP4, IP2, IP6, IP3, IP1, IP5);
        assertReplicaPlacement(replicaMap, TOKEN14, IP2, IP6, IP3, IP1, IP5, IP4);
        assertReplicaPlacement(replicaMap, TOKEN15, IP6, IP3, IP2, IP1, IP5, IP4);
        assertReplicaPlacement(replicaMap, TOKEN16, IP3, IP2, IP6, IP1, IP5, IP4);
        assertReplicaPlacement(replicaMap, TOKEN17, IP2, IP6, IP1, IP5, IP3, IP4);
        assertReplicaPlacement(replicaMap, TOKEN18, IP6, IP1, IP5, IP3, IP2, IP4);
    }
}
