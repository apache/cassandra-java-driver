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

import com.datastax.driver.core.utils.Bytes;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Set;

import static com.datastax.driver.core.Assertions.assertThat;

@CCMConfig(
        // force the initial token to a non-min value to validate that the single range will always be ]minToken, minToken]
        config = "initial_token:1",
        clusterProvider = "createClusterBuilderNoDebouncing"
)
public class SingleTokenIntegrationTest extends CCMTestsSupport {

    /**
     * JAVA-684: Empty TokenRange returned in a one token cluster
     */
    @Test(groups = "short")
    public void should_return_single_non_empty_range_when_cluster_has_one_single_token() {
        cluster().manager.controlConnection.refreshNodeListAndTokenMap();
        Metadata metadata = cluster().getMetadata();
        Set<TokenRange> tokenRanges = metadata.getTokenRanges();
        assertThat(tokenRanges).hasSize(1);
        TokenRange tokenRange = tokenRanges.iterator().next();
        assertThat(tokenRange)
                .startsWith(Token.M3PToken.FACTORY.minToken())
                .endsWith(Token.M3PToken.FACTORY.minToken())
                .isNotEmpty()
                .isNotWrappedAround();

        Set<Host> hostsForRange = metadata.getReplicas(keyspace, tokenRange);
        Host host1 = TestUtils.findHost(cluster(), 1);
        assertThat(hostsForRange).containsOnly(host1);

        ByteBuffer randomPartitionKey = Bytes.fromHexString("0xCAFEBABE");
        Set<Host> hostsForKey = metadata.getReplicas(keyspace, randomPartitionKey);
        assertThat(hostsForKey).containsOnly(host1);

        Set<TokenRange> rangesForHost = metadata.getTokenRanges(keyspace, host1);
        assertThat(rangesForHost).containsOnly(tokenRange);
    }
}
