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
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class NetworkTopologyStrategyTest extends AbstractReplicationStrategyTest {

    private static class ReplicationFactorDefinition {
        public final String dc;
        public final int replicationFactor;

        public ReplicationFactorDefinition(String dc, int replicationFactor) {
            this.dc = dc;
            this.replicationFactor = replicationFactor;
        }
    }

    private static ReplicationFactorDefinition rf(String dc, int replicationFactor) {
        return new ReplicationFactorDefinition(dc, replicationFactor);
    }

    private static ReplicationStrategy networkTopologyStrategy(ReplicationFactorDefinition... rfs) {
        Builder<String, String> builder = ImmutableMap.<String, String>builder()
                .put("class", "NetworkTopologyStrategy");

        for (ReplicationFactorDefinition rf : rfs)
            builder.put(rf.dc, String.valueOf(rf.replicationFactor));

        return ReplicationStrategy.create(builder.build());
    }

    /*
     * ---------------------------------------------------------------------------
     * Ring, replication, etc... setup. These are reusable for the tests
     * This data is based on a real ring topology. Most tests are using
     * smaller and more specific topologies instead.
     * ---------------------------------------------------------------------------
     */

    private static final String DC1 = "DC1";
    private static final String DC2 = "DC2";
    private static final String DC3 = "DC3";
    private static final String RACK11 = "RACK11";
    private static final String RACK12 = "RACK12";
    private static final String RACK21 = "RACK21";
    private static final String RACK22 = "RACK22";
    private static final String RACK31 = "RACK31";

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
    private static final InetSocketAddress IP7 = socketAddress("127.0.0.107");
    private static final InetSocketAddress IP8 = socketAddress("127.0.0.108");

    private static final ReplicationStrategy exampleStrategy = networkTopologyStrategy(rf(DC1, 2), rf(DC2, 2));

    private static final ReplicationStrategy exampleStrategyTooManyReplicas = networkTopologyStrategy(rf(DC1, 4), rf(DC2, 4));

    private static final List<Token> largeRing = Lists.newArrayList();
    private static final Map<Token, Host> largeRingTokenToPrimary = Maps.newHashMap();

    private static final String keyspace = "Excelsior";

    static {
        for (int i = 0; i < 100; i++) {
            InetSocketAddress address = socketAddress("127.0.0." + i);
            for (int vnodes = 0; vnodes < 256; vnodes++) {
                Token token = token("" + ((i * 256) + vnodes));
                largeRing.add(token);
                largeRingTokenToPrimary.put(token, host(address, DC1, RACK11));
            }
        }
    }

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
            .put(TOKEN01, host(IP1, DC1, RACK11))
            .put(TOKEN02, host(IP1, DC1, RACK11))
            .put(TOKEN03, host(IP5, DC1, RACK12))
            .put(TOKEN04, host(IP3, DC1, RACK11))
            .put(TOKEN05, host(IP1, DC1, RACK11))
            .put(TOKEN06, host(IP5, DC1, RACK12))
            .put(TOKEN07, host(IP2, DC2, RACK21))
            .put(TOKEN08, host(IP6, DC2, RACK22))
            .put(TOKEN09, host(IP3, DC1, RACK11))
            .put(TOKEN10, host(IP4, DC2, RACK21))
            .put(TOKEN11, host(IP5, DC1, RACK12))
            .put(TOKEN12, host(IP4, DC2, RACK21))
            .put(TOKEN13, host(IP4, DC2, RACK21))
            .put(TOKEN14, host(IP2, DC2, RACK21))
            .put(TOKEN15, host(IP6, DC2, RACK22))
            .put(TOKEN16, host(IP3, DC1, RACK11))
            .put(TOKEN17, host(IP2, DC2, RACK21))
            .put(TOKEN18, host(IP6, DC2, RACK22))
            .build();

    /*
     * --------------
     *     Tests
     * --------------
     */

    @Test(groups = "unit")
    public void networkTopologyWithSimpleDCLayoutTest1() {
        List<Token> ring = ImmutableList.<Token>builder()
                .add(TOKEN01)
                .add(TOKEN04)
                .add(TOKEN14)
                .add(TOKEN19)
                .build();

        Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
                .put(TOKEN01, host(IP1, DC1, RACK11))
                .put(TOKEN04, host(IP2, DC2, RACK21))
                .put(TOKEN14, host(IP1, DC1, RACK11))
                .put(TOKEN19, host(IP2, DC2, RACK21))
                .build();

        ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 1), rf(DC2, 1));

        Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(keyspace, tokenToPrimary, ring);

        assertReplicaPlacement(replicaMap, TOKEN01, IP1, IP2);
        assertReplicaPlacement(replicaMap, TOKEN04, IP2, IP1);
        assertReplicaPlacement(replicaMap, TOKEN14, IP1, IP2);
        assertReplicaPlacement(replicaMap, TOKEN19, IP2, IP1);
    }

    @Test(groups = "unit")
    public void networkTopologyWithSimpleDCLayoutTest2() {
        List<Token> ring = ImmutableList.<Token>builder()
                .add(TOKEN01)
                .add(TOKEN03)
                .add(TOKEN05)
                .add(TOKEN07)
                .add(TOKEN13)
                .add(TOKEN15)
                .add(TOKEN17)
                .add(TOKEN19)
                .build();

        Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
                .put(TOKEN01, host(IP1, DC1, RACK11))
                .put(TOKEN03, host(IP2, DC2, RACK21))
                .put(TOKEN05, host(IP3, DC1, RACK11))
                .put(TOKEN07, host(IP4, DC2, RACK21))
                .put(TOKEN13, host(IP1, DC1, RACK11))
                .put(TOKEN15, host(IP2, DC2, RACK21))
                .put(TOKEN17, host(IP3, DC1, RACK11))
                .put(TOKEN19, host(IP4, DC2, RACK21))
                .build();

        ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 1), rf(DC2, 1));

        Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(keyspace, tokenToPrimary, ring);

        assertReplicaPlacement(replicaMap, TOKEN01, IP1, IP2);
        assertReplicaPlacement(replicaMap, TOKEN03, IP2, IP3);
        assertReplicaPlacement(replicaMap, TOKEN05, IP3, IP4);
        assertReplicaPlacement(replicaMap, TOKEN07, IP4, IP1);
        assertReplicaPlacement(replicaMap, TOKEN13, IP1, IP2);
        assertReplicaPlacement(replicaMap, TOKEN15, IP2, IP3);
        assertReplicaPlacement(replicaMap, TOKEN17, IP3, IP4);
        assertReplicaPlacement(replicaMap, TOKEN19, IP4, IP1);
    }

    @Test(groups = "unit")
    public void networkTopologyWithSimple3DCLayoutTest() {
        List<Token> ring = ImmutableList.<Token>builder()
                .add(TOKEN01)
                .add(TOKEN05)
                .add(TOKEN09)
                .add(TOKEN11)
                .add(TOKEN15)
                .add(TOKEN19)
                .build();

        Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
                .put(TOKEN01, host(IP1, DC1, RACK11))
                .put(TOKEN05, host(IP2, DC2, RACK21))
                .put(TOKEN09, host(IP3, DC3, RACK31))
                .put(TOKEN11, host(IP1, DC1, RACK11))
                .put(TOKEN15, host(IP2, DC2, RACK21))
                .put(TOKEN19, host(IP3, DC3, RACK31))
                .build();

        ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 1), rf(DC2, 1), rf(DC3, 1));

        Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(keyspace, tokenToPrimary, ring);

        assertReplicaPlacement(replicaMap, TOKEN01, IP1, IP2, IP3);
        assertReplicaPlacement(replicaMap, TOKEN05, IP2, IP3, IP1);
        assertReplicaPlacement(replicaMap, TOKEN09, IP3, IP1, IP2);
        assertReplicaPlacement(replicaMap, TOKEN11, IP1, IP2, IP3);
        assertReplicaPlacement(replicaMap, TOKEN15, IP2, IP3, IP1);
        assertReplicaPlacement(replicaMap, TOKEN19, IP3, IP1, IP2);
    }

    @Test(groups = "unit")
    public void networkTopologyWithUnbalancedRingTest() {
        List<Token> ring = ImmutableList.<Token>builder()
                .add(TOKEN01)
                .add(TOKEN03)
                .add(TOKEN05)
                .add(TOKEN07)
                .add(TOKEN09)
                .add(TOKEN11)
                .add(TOKEN13)
                .add(TOKEN15)
                .add(TOKEN17)
                .add(TOKEN19)
                .build();

        Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
                .put(TOKEN01, host(IP1, DC1, RACK11))
                .put(TOKEN03, host(IP1, DC1, RACK11))
                .put(TOKEN05, host(IP2, DC2, RACK21))
                .put(TOKEN07, host(IP3, DC1, RACK11))
                .put(TOKEN09, host(IP4, DC2, RACK21))
                .put(TOKEN11, host(IP1, DC1, RACK11))
                .put(TOKEN13, host(IP1, DC1, RACK11))
                .put(TOKEN15, host(IP2, DC2, RACK21))
                .put(TOKEN17, host(IP3, DC1, RACK11))
                .put(TOKEN19, host(IP4, DC2, RACK21))
                .build();

        ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 2), rf(DC2, 2));

        Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(keyspace, tokenToPrimary, ring);

        assertReplicaPlacement(replicaMap, TOKEN01, IP1, IP2, IP3, IP4);
        assertReplicaPlacement(replicaMap, TOKEN03, IP1, IP2, IP3, IP4);
        assertReplicaPlacement(replicaMap, TOKEN05, IP2, IP3, IP4, IP1);
        assertReplicaPlacement(replicaMap, TOKEN07, IP3, IP4, IP1, IP2);
        assertReplicaPlacement(replicaMap, TOKEN09, IP4, IP1, IP2, IP3);
        assertReplicaPlacement(replicaMap, TOKEN11, IP1, IP2, IP3, IP4);
        assertReplicaPlacement(replicaMap, TOKEN13, IP1, IP2, IP3, IP4);
        assertReplicaPlacement(replicaMap, TOKEN15, IP2, IP3, IP4, IP1);
        assertReplicaPlacement(replicaMap, TOKEN17, IP3, IP4, IP1, IP2);
        assertReplicaPlacement(replicaMap, TOKEN19, IP4, IP1, IP2, IP3);
    }

    @Test(groups = "unit")
    public void networkTopologyWithDCMultirackLayoutTest() {
        List<Token> ring = ImmutableList.<Token>builder()
                .add(TOKEN01)
                .add(TOKEN02)
                .add(TOKEN03)
                .add(TOKEN04)
                .add(TOKEN05)
                .add(TOKEN06)
                .add(TOKEN07)
                .add(TOKEN08)
                .add(TOKEN12)
                .add(TOKEN13)
                .add(TOKEN14)
                .add(TOKEN15)
                .add(TOKEN16)
                .add(TOKEN17)
                .add(TOKEN18)
                .add(TOKEN19)
                .build();

        Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
                .put(TOKEN01, host(IP1, DC1, RACK11))
                .put(TOKEN02, host(IP2, DC2, RACK21))
                .put(TOKEN03, host(IP3, DC1, RACK12))
                .put(TOKEN04, host(IP4, DC2, RACK22))
                .put(TOKEN05, host(IP5, DC1, RACK11))
                .put(TOKEN06, host(IP6, DC2, RACK21))
                .put(TOKEN07, host(IP7, DC1, RACK12))
                .put(TOKEN08, host(IP8, DC2, RACK22))
                .put(TOKEN12, host(IP1, DC1, RACK11))
                .put(TOKEN13, host(IP2, DC2, RACK21))
                .put(TOKEN14, host(IP3, DC1, RACK12))
                .put(TOKEN15, host(IP4, DC2, RACK22))
                .put(TOKEN16, host(IP5, DC1, RACK11))
                .put(TOKEN17, host(IP6, DC2, RACK21))
                .put(TOKEN18, host(IP7, DC1, RACK12))
                .put(TOKEN19, host(IP8, DC2, RACK22))
                .build();

        ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 2), rf(DC2, 2));

        Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(keyspace, tokenToPrimary, ring);

        assertReplicaPlacement(replicaMap, TOKEN01, IP1, IP2, IP3, IP4);
        assertReplicaPlacement(replicaMap, TOKEN02, IP2, IP3, IP4, IP5);
        assertReplicaPlacement(replicaMap, TOKEN03, IP3, IP4, IP5, IP6);
        assertReplicaPlacement(replicaMap, TOKEN04, IP4, IP5, IP6, IP7);
        assertReplicaPlacement(replicaMap, TOKEN05, IP5, IP6, IP7, IP8);
        assertReplicaPlacement(replicaMap, TOKEN06, IP6, IP7, IP8, IP1);
        assertReplicaPlacement(replicaMap, TOKEN07, IP7, IP8, IP1, IP2);
        assertReplicaPlacement(replicaMap, TOKEN08, IP8, IP1, IP2, IP3);
        assertReplicaPlacement(replicaMap, TOKEN12, IP1, IP2, IP3, IP4);
        assertReplicaPlacement(replicaMap, TOKEN13, IP2, IP3, IP4, IP5);
        assertReplicaPlacement(replicaMap, TOKEN14, IP3, IP4, IP5, IP6);
        assertReplicaPlacement(replicaMap, TOKEN15, IP4, IP5, IP6, IP7);
        assertReplicaPlacement(replicaMap, TOKEN16, IP5, IP6, IP7, IP8);
        assertReplicaPlacement(replicaMap, TOKEN17, IP6, IP7, IP8, IP1);
        assertReplicaPlacement(replicaMap, TOKEN18, IP7, IP8, IP1, IP2);
        assertReplicaPlacement(replicaMap, TOKEN19, IP8, IP1, IP2, IP3);
    }

    @Test(groups = "unit")
    public void networkTopologyWithMultirackHostSkippingTest1() {
        List<Token> ring = ImmutableList.<Token>builder()
                .add(TOKEN01)
                .add(TOKEN02)
                .add(TOKEN03)
                .add(TOKEN04)
                .add(TOKEN05)
                .add(TOKEN06)
                .add(TOKEN07)
                .add(TOKEN08)
                .add(TOKEN12)
                .add(TOKEN13)
                .add(TOKEN14)
                .add(TOKEN15)
                .add(TOKEN16)
                .add(TOKEN17)
                .add(TOKEN18)
                .add(TOKEN19)
                .build();

        //this is to simulate when we hit the same rack in a DC first as a second replica
        //so that'll get skipped and re-added later as a third
        Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
                .put(TOKEN01, host(IP1, DC1, RACK11))
                .put(TOKEN02, host(IP2, DC2, RACK21))
                .put(TOKEN03, host(IP3, DC1, RACK11))
                .put(TOKEN04, host(IP4, DC2, RACK21))
                .put(TOKEN05, host(IP5, DC1, RACK12))
                .put(TOKEN06, host(IP6, DC2, RACK22))
                .put(TOKEN07, host(IP7, DC1, RACK12))
                .put(TOKEN08, host(IP8, DC2, RACK22))
                .put(TOKEN12, host(IP1, DC1, RACK11))
                .put(TOKEN13, host(IP2, DC2, RACK21))
                .put(TOKEN14, host(IP3, DC1, RACK11))
                .put(TOKEN15, host(IP4, DC2, RACK21))
                .put(TOKEN16, host(IP5, DC1, RACK12))
                .put(TOKEN17, host(IP6, DC2, RACK22))
                .put(TOKEN18, host(IP7, DC1, RACK12))
                .put(TOKEN19, host(IP8, DC2, RACK22))
                .build();

        ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 3), rf(DC2, 3));

        Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(keyspace, tokenToPrimary, ring);

        assertReplicaPlacement(replicaMap, TOKEN01, IP1, IP2, IP5, IP3, IP6, IP4);
        assertReplicaPlacement(replicaMap, TOKEN02, IP2, IP3, IP5, IP6, IP4, IP7);
        assertReplicaPlacement(replicaMap, TOKEN03, IP3, IP4, IP5, IP6, IP7, IP8);
        assertReplicaPlacement(replicaMap, TOKEN04, IP4, IP5, IP6, IP8, IP1, IP7);
        assertReplicaPlacement(replicaMap, TOKEN05, IP5, IP6, IP1, IP7, IP2, IP8);
        assertReplicaPlacement(replicaMap, TOKEN06, IP6, IP7, IP1, IP2, IP8, IP3);
        assertReplicaPlacement(replicaMap, TOKEN07, IP7, IP8, IP1, IP2, IP3, IP4);
        assertReplicaPlacement(replicaMap, TOKEN08, IP8, IP1, IP2, IP4, IP5, IP3);
        assertReplicaPlacement(replicaMap, TOKEN12, IP1, IP2, IP5, IP3, IP6, IP4);
        assertReplicaPlacement(replicaMap, TOKEN13, IP2, IP3, IP5, IP6, IP4, IP7);
        assertReplicaPlacement(replicaMap, TOKEN14, IP3, IP4, IP5, IP6, IP7, IP8);
        assertReplicaPlacement(replicaMap, TOKEN15, IP4, IP5, IP6, IP8, IP1, IP7);
        assertReplicaPlacement(replicaMap, TOKEN16, IP5, IP6, IP1, IP7, IP2, IP8);
        assertReplicaPlacement(replicaMap, TOKEN17, IP6, IP7, IP1, IP2, IP8, IP3);
        assertReplicaPlacement(replicaMap, TOKEN18, IP7, IP8, IP1, IP2, IP3, IP4);
        assertReplicaPlacement(replicaMap, TOKEN19, IP8, IP1, IP2, IP4, IP5, IP3);

    }

    @Test(groups = "unit")
    public void networkTopologyWithMultirackHostSkippingTest2() {
        List<Token> ring = ImmutableList.<Token>builder()
                .add(TOKEN01)
                .add(TOKEN02)
                .add(TOKEN03)
                .add(TOKEN04)
                .add(TOKEN05)
                .add(TOKEN06)
                .add(TOKEN07)
                .add(TOKEN08)
                .add(TOKEN12)
                .add(TOKEN13)
                .add(TOKEN14)
                .add(TOKEN15)
                .add(TOKEN16)
                .add(TOKEN17)
                .add(TOKEN18)
                .add(TOKEN19)
                .build();

        Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
                .put(TOKEN01, host(IP1, DC1, RACK11))
                .put(TOKEN02, host(IP1, DC1, RACK11))
                .put(TOKEN03, host(IP3, DC1, RACK11))
                .put(TOKEN04, host(IP3, DC1, RACK11))
                .put(TOKEN05, host(IP5, DC1, RACK12))
                .put(TOKEN06, host(IP5, DC1, RACK12))
                .put(TOKEN07, host(IP7, DC1, RACK12))
                .put(TOKEN08, host(IP7, DC1, RACK12))
                .put(TOKEN12, host(IP2, DC2, RACK21))
                .put(TOKEN13, host(IP2, DC2, RACK21))
                .put(TOKEN14, host(IP4, DC2, RACK21))
                .put(TOKEN15, host(IP4, DC2, RACK21))
                .put(TOKEN16, host(IP6, DC2, RACK22))
                .put(TOKEN17, host(IP6, DC2, RACK22))
                .put(TOKEN18, host(IP8, DC2, RACK22))
                .put(TOKEN19, host(IP8, DC2, RACK22))
                .build();

        ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 3), rf(DC2, 3));

        Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(keyspace, tokenToPrimary, ring);

        assertReplicaPlacement(replicaMap, TOKEN01, IP1, IP5, IP3, IP2, IP6, IP4);
        assertReplicaPlacement(replicaMap, TOKEN02, IP1, IP5, IP3, IP2, IP6, IP4);
        assertReplicaPlacement(replicaMap, TOKEN03, IP3, IP5, IP7, IP2, IP6, IP4);
        assertReplicaPlacement(replicaMap, TOKEN04, IP3, IP5, IP7, IP2, IP6, IP4);
        assertReplicaPlacement(replicaMap, TOKEN05, IP5, IP2, IP6, IP4, IP1, IP7);
        assertReplicaPlacement(replicaMap, TOKEN06, IP5, IP2, IP6, IP4, IP1, IP7);
        assertReplicaPlacement(replicaMap, TOKEN07, IP7, IP2, IP6, IP4, IP1, IP3);
        assertReplicaPlacement(replicaMap, TOKEN08, IP7, IP2, IP6, IP4, IP1, IP3);
        assertReplicaPlacement(replicaMap, TOKEN12, IP2, IP6, IP4, IP1, IP5, IP3);
        assertReplicaPlacement(replicaMap, TOKEN13, IP2, IP6, IP4, IP1, IP5, IP3);
        assertReplicaPlacement(replicaMap, TOKEN14, IP4, IP6, IP8, IP1, IP5, IP3);
        assertReplicaPlacement(replicaMap, TOKEN15, IP4, IP6, IP8, IP1, IP5, IP3);
        assertReplicaPlacement(replicaMap, TOKEN16, IP6, IP1, IP5, IP3, IP2, IP8);
        assertReplicaPlacement(replicaMap, TOKEN17, IP6, IP1, IP5, IP3, IP2, IP8);
        assertReplicaPlacement(replicaMap, TOKEN18, IP8, IP1, IP5, IP3, IP2, IP4);
        assertReplicaPlacement(replicaMap, TOKEN19, IP8, IP1, IP5, IP3, IP2, IP4);
    }

    @Test(groups = "unit")
    public void networkTopologyWithMultirackHostSkippingTest3() {
        //this is the same topology as in the previous test, but with different rfs
        List<Token> ring = ImmutableList.<Token>builder()
                .add(TOKEN01)
                .add(TOKEN02)
                .add(TOKEN03)
                .add(TOKEN04)
                .add(TOKEN05)
                .add(TOKEN06)
                .add(TOKEN07)
                .add(TOKEN08)
                .add(TOKEN12)
                .add(TOKEN13)
                .add(TOKEN14)
                .add(TOKEN15)
                .add(TOKEN16)
                .add(TOKEN17)
                .add(TOKEN18)
                .add(TOKEN19)
                .build();

        Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
                .put(TOKEN01, host(IP1, DC1, RACK11))
                .put(TOKEN02, host(IP1, DC1, RACK11))
                .put(TOKEN03, host(IP3, DC1, RACK11))
                .put(TOKEN04, host(IP3, DC1, RACK11))
                .put(TOKEN05, host(IP5, DC1, RACK12))
                .put(TOKEN06, host(IP5, DC1, RACK12))
                .put(TOKEN07, host(IP7, DC1, RACK12))
                .put(TOKEN08, host(IP7, DC1, RACK12))
                .put(TOKEN12, host(IP2, DC2, RACK21))
                .put(TOKEN13, host(IP2, DC2, RACK21))
                .put(TOKEN14, host(IP4, DC2, RACK21))
                .put(TOKEN15, host(IP4, DC2, RACK21))
                .put(TOKEN16, host(IP6, DC2, RACK22))
                .put(TOKEN17, host(IP6, DC2, RACK22))
                .put(TOKEN18, host(IP8, DC2, RACK22))
                .put(TOKEN19, host(IP8, DC2, RACK22))
                .build();

        //all nodes will contain all data, question is the replica order
        ReplicationStrategy strategy = networkTopologyStrategy(rf(DC1, 4), rf(DC2, 4));

        Map<Token, Set<Host>> replicaMap = strategy.computeTokenToReplicaMap(keyspace, tokenToPrimary, ring);

        assertReplicaPlacement(replicaMap, TOKEN01, IP1, IP5, IP3, IP7, IP2, IP6, IP4, IP8);
        assertReplicaPlacement(replicaMap, TOKEN02, IP1, IP5, IP3, IP7, IP2, IP6, IP4, IP8);
        assertReplicaPlacement(replicaMap, TOKEN03, IP3, IP5, IP7, IP2, IP6, IP4, IP8, IP1);
        assertReplicaPlacement(replicaMap, TOKEN04, IP3, IP5, IP7, IP2, IP6, IP4, IP8, IP1);
        assertReplicaPlacement(replicaMap, TOKEN05, IP5, IP2, IP6, IP4, IP8, IP1, IP7, IP3);
        assertReplicaPlacement(replicaMap, TOKEN06, IP5, IP2, IP6, IP4, IP8, IP1, IP7, IP3);
        assertReplicaPlacement(replicaMap, TOKEN07, IP7, IP2, IP6, IP4, IP8, IP1, IP3, IP5);
        assertReplicaPlacement(replicaMap, TOKEN08, IP7, IP2, IP6, IP4, IP8, IP1, IP3, IP5);
        assertReplicaPlacement(replicaMap, TOKEN12, IP2, IP6, IP4, IP8, IP1, IP5, IP3, IP7);
        assertReplicaPlacement(replicaMap, TOKEN13, IP2, IP6, IP4, IP8, IP1, IP5, IP3, IP7);
        assertReplicaPlacement(replicaMap, TOKEN14, IP4, IP6, IP8, IP1, IP5, IP3, IP7, IP2);
        assertReplicaPlacement(replicaMap, TOKEN15, IP4, IP6, IP8, IP1, IP5, IP3, IP7, IP2);
        assertReplicaPlacement(replicaMap, TOKEN16, IP6, IP1, IP5, IP3, IP7, IP2, IP8, IP4);
        assertReplicaPlacement(replicaMap, TOKEN17, IP6, IP1, IP5, IP3, IP7, IP2, IP8, IP4);
        assertReplicaPlacement(replicaMap, TOKEN18, IP8, IP1, IP5, IP3, IP7, IP2, IP4, IP6);
        assertReplicaPlacement(replicaMap, TOKEN19, IP8, IP1, IP5, IP3, IP7, IP2, IP4, IP6);
    }

    @Test(groups = "unit")
    public void networkTopologyStrategyExampleTopologyTest() {
        Map<Token, Set<Host>> replicaMap = exampleStrategy.computeTokenToReplicaMap(keyspace, exampleTokenToPrimary, exampleRing);

        //105 and 106 will appear as replica for all as they're in separate racks
        assertReplicaPlacement(replicaMap, TOKEN01, IP1, IP5, IP2, IP6);
        assertReplicaPlacement(replicaMap, TOKEN02, IP1, IP5, IP2, IP6);
        assertReplicaPlacement(replicaMap, TOKEN03, IP5, IP3, IP2, IP6);
        assertReplicaPlacement(replicaMap, TOKEN04, IP3, IP5, IP2, IP6);
        assertReplicaPlacement(replicaMap, TOKEN05, IP1, IP5, IP2, IP6);
        assertReplicaPlacement(replicaMap, TOKEN06, IP5, IP2, IP6, IP3);
        assertReplicaPlacement(replicaMap, TOKEN07, IP2, IP6, IP3, IP5);
        assertReplicaPlacement(replicaMap, TOKEN08, IP6, IP3, IP4, IP5);
        assertReplicaPlacement(replicaMap, TOKEN09, IP3, IP4, IP5, IP6);
        assertReplicaPlacement(replicaMap, TOKEN10, IP4, IP5, IP6, IP3);
        assertReplicaPlacement(replicaMap, TOKEN11, IP5, IP4, IP6, IP3);
        assertReplicaPlacement(replicaMap, TOKEN12, IP4, IP6, IP3, IP5);
        assertReplicaPlacement(replicaMap, TOKEN13, IP4, IP6, IP3, IP5);
        assertReplicaPlacement(replicaMap, TOKEN14, IP2, IP6, IP3, IP5);
        assertReplicaPlacement(replicaMap, TOKEN15, IP6, IP3, IP2, IP5);
        assertReplicaPlacement(replicaMap, TOKEN16, IP3, IP2, IP6, IP5);
        assertReplicaPlacement(replicaMap, TOKEN17, IP2, IP6, IP1, IP5);
        assertReplicaPlacement(replicaMap, TOKEN18, IP6, IP1, IP5, IP2);
    }

    @Test(groups = "unit")
    public void networkTopologyStrategyNoNodesInDCTest() {
        long t1 = System.currentTimeMillis();
        Map<Token, Set<Host>> replicaMap = networkTopologyStrategy(rf(DC1, 2), rf(DC2, 2))
                .computeTokenToReplicaMap(keyspace, largeRingTokenToPrimary, largeRing);
        assertThat(System.currentTimeMillis() - t1).isLessThan(10000);

        InetSocketAddress currNode = null;
        InetSocketAddress nextNode;
        for (int node = 0; node < 99; node++) { // 100th wraps so doesn't match this, check after
            if (currNode == null) {
                currNode = socketAddress("127.0.0." + node);
            }
            nextNode = socketAddress("127.0.0." + (node + 1));
            for (int vnodes = 0; vnodes < 256; vnodes++) {
                Token token = token("" + ((node * 256) + vnodes));
                assertReplicaPlacement(replicaMap, token, currNode, nextNode);
            }
            currNode = nextNode;
        }
        assertReplicaPlacement(replicaMap, token("" + 99 * 256), currNode, socketAddress("127.0.0.0"));
    }


    @Test(groups = "unit")
    public void networkTopologyStrategyExampleTopologyTooManyReplicasTest() {
        Map<Token, Set<Host>> replicaMap = exampleStrategyTooManyReplicas.computeTokenToReplicaMap(keyspace, exampleTokenToPrimary, exampleRing);

        assertReplicaPlacement(replicaMap, TOKEN01, IP1, IP5, IP3, IP2, IP6, IP4);
        assertReplicaPlacement(replicaMap, TOKEN02, IP1, IP5, IP3, IP2, IP6, IP4);
        assertReplicaPlacement(replicaMap, TOKEN03, IP5, IP3, IP1, IP2, IP6, IP4);
        assertReplicaPlacement(replicaMap, TOKEN04, IP3, IP5, IP1, IP2, IP6, IP4);
        assertReplicaPlacement(replicaMap, TOKEN05, IP1, IP5, IP2, IP6, IP3, IP4);
        assertReplicaPlacement(replicaMap, TOKEN06, IP5, IP2, IP6, IP3, IP4, IP1);
        assertReplicaPlacement(replicaMap, TOKEN07, IP2, IP6, IP3, IP4, IP5, IP1);
        assertReplicaPlacement(replicaMap, TOKEN08, IP6, IP3, IP4, IP5, IP2, IP1);
        assertReplicaPlacement(replicaMap, TOKEN09, IP3, IP4, IP5, IP6, IP2, IP1);
        assertReplicaPlacement(replicaMap, TOKEN10, IP4, IP5, IP6, IP2, IP3, IP1);
        assertReplicaPlacement(replicaMap, TOKEN11, IP5, IP4, IP6, IP2, IP3, IP1);
        assertReplicaPlacement(replicaMap, TOKEN12, IP4, IP6, IP2, IP3, IP5, IP1);
        assertReplicaPlacement(replicaMap, TOKEN13, IP4, IP6, IP2, IP3, IP5, IP1);
        assertReplicaPlacement(replicaMap, TOKEN14, IP2, IP6, IP3, IP5, IP1, IP4);
        assertReplicaPlacement(replicaMap, TOKEN15, IP6, IP3, IP2, IP5, IP1, IP4);
        assertReplicaPlacement(replicaMap, TOKEN16, IP3, IP2, IP6, IP5, IP1, IP4);
        assertReplicaPlacement(replicaMap, TOKEN17, IP2, IP6, IP1, IP5, IP3, IP4);
        assertReplicaPlacement(replicaMap, TOKEN18, IP6, IP1, IP5, IP3, IP2, IP4);
    }

    @Test(groups = "unit")
    public void should_warn_if_replication_factor_cannot_be_met() {
        Logger logger = Logger.getLogger(ReplicationStrategy.NetworkTopologyStrategy.class);
        MemoryAppender logs = new MemoryAppender();
        Level originalLevel = logger.getLevel();
        try {
            logger.setLevel(Level.WARN);
            logger.addAppender(logs);

            List<Token> ring = ImmutableList.<Token>builder()
                    .add(TOKEN01)
                    .add(TOKEN02)
                    .add(TOKEN03)
                    .add(TOKEN04)
                    .build();

            Map<Token, Host> tokenToPrimary = ImmutableMap.<Token, Host>builder()
                    .put(TOKEN01, host(IP1, DC1, RACK11))
                    .put(TOKEN02, host(IP2, DC1, RACK12))
                    .put(TOKEN03, host(IP3, DC2, RACK21))
                    .put(TOKEN04, host(IP4, DC2, RACK22))
                    .build();

            // Wrong configuration: impossible replication factor for DC2
            networkTopologyStrategy(rf(DC1, 2), rf(DC2, 3))
                    .computeTokenToReplicaMap(keyspace, tokenToPrimary, ring);
            assertThat(logs.getNext())
                    .contains(String.format("Error while computing token map for keyspace %s with datacenter %s", keyspace, DC2));

            // Wrong configuration: non-existing datacenter
            networkTopologyStrategy(rf(DC1, 2), rf("does_not_exist", 2))
                    .computeTokenToReplicaMap(keyspace, tokenToPrimary, ring);
            assertThat(logs.getNext())
                    .contains(String.format("Error while computing token map for keyspace %s with datacenter %s", keyspace, "does_not_exist"));
        } finally {
            logger.setLevel(originalLevel);
            logger.removeAppender(logs);
        }
    }
}
