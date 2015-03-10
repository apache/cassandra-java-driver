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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.testng.annotations.*;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.assertj.core.api.Assertions.*;

import com.datastax.driver.core.CCMBridge.CCMCluster;

import static com.datastax.driver.core.Token.M3PToken.FACTORY;

/**
 * Tests for {@link com.datastax.driver.core.Metadata.TokenMap}.
 */
public class TokenMapTest {
    CCMCluster ccmCluster = null;
    Cluster cluster = null;

    @BeforeMethod(groups = "short")
    public void setup() {
        ccmCluster = CCMBridge.buildCluster(1, Cluster.builder());
        cluster = ccmCluster.cluster;
    }

    /**
     * Tests that the token map is correctly initialized at startup (JAVA-415).
     */
    @Test(groups = "short")
    public void initTest() {
        // If the token map is initialized correctly, we should get replicas for any partition key
        ByteBuffer anyKey = ByteBuffer.wrap(new byte[]{});
        Set<Host> replicas = cluster.getMetadata().getReplicas("system", anyKey);
        assertFalse(replicas.isEmpty());
    }

    /**
     * DEVC-684: Empty TokenRange returned in a one token cluster
     */
    @Test(groups = "unit")
    public void should_return_single_non_empty_range_when_cluster_has_one_single_token() {
        // given a single node cluster with one single token "1"
        // ("1" is purposely chosen because it is not the minimum token for the partitioner in use)
        Collection<String> tokens = Sets.newHashSet("1");
        Map<Host, Collection<String>> allTokens = ImmutableMap.of(mock(Host.class), tokens);
        // when the token map is built
        Metadata metadata = new Metadata(mock(Cluster.Manager.class));
        metadata.rebuildTokenMap("Murmur3Partitioner", allTokens);
        // then
        Set<TokenRange> tokenRanges = metadata.getTokenRanges();
        assertThat(tokenRanges).hasSize(1);
        TokenRange tokenRange = tokenRanges.iterator().next();
        assertThat(tokenRange.getStart()).isEqualTo(FACTORY.minToken());
        assertThat(tokenRange.getEnd()).isEqualTo(FACTORY.minToken());
        assertThat(tokenRange.isEmpty()).isFalse();
        assertThat(tokenRange.isWrappedAround()).isFalse();
    }

    @AfterMethod(groups = "short")
    public void teardown() {
        ccmCluster.discard();
    }
}
