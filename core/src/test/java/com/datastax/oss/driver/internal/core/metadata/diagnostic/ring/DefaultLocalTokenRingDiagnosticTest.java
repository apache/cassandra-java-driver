/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.internal.core.metadata.diagnostic.ring;

import static com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_QUORUM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.diagnostic.Status;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TokenRangeDiagnostic;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultReplicationStrategyFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultLocalTokenRingDiagnosticTest {

  @Mock(name = "tr1")
  TokenRange tr1;

  @Mock(name = "tr2")
  TokenRange tr2;

  @Mock(name = "ks1")
  KeyspaceMetadata ks;

  Map<String, String> replication =
      ImmutableMap.of(
          "class",
          DefaultReplicationStrategyFactory.NETWORK_TOPOLOGY_STRATEGY,
          "dc1",
          "3",
          "dc2",
          "1");

  @Before
  public void setUp() {
    given(tr1.format()).willReturn("]1,2]");
    given(tr2.compareTo(tr1)).willReturn(1);
    given(ks.getName()).willReturn(CqlIdentifier.fromInternal("ks1"));
    given(ks.getReplication()).willReturn(replication);
  }

  @Test
  public void should_create_diagnostic_for_local_CL() {
    // given
    Set<TokenRangeDiagnostic> trDiagnostics =
        ImmutableSet.of(
            new SimpleTokenRangeDiagnostic(tr1, ks, LOCAL_QUORUM, 2, 1),
            new SimpleTokenRangeDiagnostic(tr2, ks, LOCAL_QUORUM, 2, 3));
    // when
    DefaultLocalTokenRingDiagnostic diagnostic =
        new DefaultLocalTokenRingDiagnostic(ks, LOCAL_QUORUM, "dc1", trDiagnostics);
    // then
    assertThat(diagnostic.getKeyspace()).isEqualTo(ks);
    assertThat(diagnostic.getConsistencyLevel()).isEqualTo(LOCAL_QUORUM);
    assertThat(diagnostic.getDatacenter()).isEqualTo("dc1");
    assertThat(diagnostic.getTokenRangeDiagnostics()).isEqualTo(trDiagnostics);
    assertThat(diagnostic.getStatus()).isEqualTo(Status.UNAVAILABLE);
    assertThat(diagnostic.getDetails())
        .isEqualTo(
            ImmutableMap.<String, Object>builder()
                .put("status", Status.UNAVAILABLE)
                .put("consistencyLevel", LOCAL_QUORUM)
                .put("keyspace", "ks1")
                .put("replication", replication)
                .put("datacenter", "dc1")
                .put("availableRanges", 1)
                .put("unavailableRanges", 1)
                .put(
                    "top10UnavailableRanges",
                    ImmutableMap.of("]1,2]", ImmutableMap.of("alive", 1, "required", 2)))
                .build());
  }
}
