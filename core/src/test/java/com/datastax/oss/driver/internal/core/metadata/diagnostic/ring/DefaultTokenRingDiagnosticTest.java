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

import static com.datastax.oss.driver.api.core.ConsistencyLevel.EACH_QUORUM;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.QUORUM;
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
public class DefaultTokenRingDiagnosticTest {

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
  public void should_create_diagnostic_for_non_local_CL() {
    // given
    Set<TokenRangeDiagnostic> trDiagnostics =
        ImmutableSet.of(
            new SimpleTokenRangeDiagnostic(tr1, ks, QUORUM, 2, 1),
            new SimpleTokenRangeDiagnostic(tr2, ks, QUORUM, 2, 3));
    // when
    DefaultTokenRingDiagnostic diagnostic =
        new DefaultTokenRingDiagnostic(ks, QUORUM, trDiagnostics);
    // then
    assertThat(diagnostic.getKeyspace()).isEqualTo(ks);
    assertThat(diagnostic.getConsistencyLevel()).isEqualTo(QUORUM);
    assertThat(diagnostic.getTokenRangeDiagnostics()).isEqualTo(trDiagnostics);
    assertThat(diagnostic.getStatus()).isEqualTo(Status.UNAVAILABLE);
    assertThat(diagnostic.getDetails())
        .isEqualTo(
            ImmutableMap.<String, Object>builder()
                .put("status", Status.UNAVAILABLE)
                .put("keyspace", "ks1")
                .put("replication", replication)
                .put("consistencyLevel", QUORUM)
                .put("availableRanges", 1)
                .put("unavailableRanges", 1)
                .put(
                    "top10UnavailableRanges",
                    ImmutableMap.of("]1,2]", ImmutableMap.of("alive", 1, "required", 2)))
                .build());
  }

  @Test
  public void should_create_diagnostic_for_EACH_QUORUM() {
    // given
    Set<TokenRangeDiagnostic> trDiagnostics =
        ImmutableSet.of(
            new CompositeTokenRangeDiagnostic(
                tr1,
                ks,
                EACH_QUORUM,
                ImmutableMap.of(
                    "dc1", new SimpleTokenRangeDiagnostic(tr1, ks, EACH_QUORUM, 2, 1),
                    "dc2", new SimpleTokenRangeDiagnostic(tr1, ks, EACH_QUORUM, 1, 0))),
            new CompositeTokenRangeDiagnostic(
                tr2,
                ks,
                EACH_QUORUM,
                ImmutableMap.of(
                    "dc1", new SimpleTokenRangeDiagnostic(tr2, ks, EACH_QUORUM, 2, 3),
                    "dc2", new SimpleTokenRangeDiagnostic(tr2, ks, EACH_QUORUM, 1, 1))));
    // when
    DefaultTokenRingDiagnostic diagnostic =
        new DefaultTokenRingDiagnostic(ks, EACH_QUORUM, trDiagnostics);
    // then
    assertThat(diagnostic.getKeyspace()).isEqualTo(ks);
    assertThat(diagnostic.getConsistencyLevel()).isEqualTo(EACH_QUORUM);
    assertThat(diagnostic.getTokenRangeDiagnostics()).isEqualTo(trDiagnostics);
    assertThat(diagnostic.getStatus()).isEqualTo(Status.UNAVAILABLE);
    assertThat(diagnostic.getDetails())
        .isEqualTo(
            ImmutableMap.<String, Object>builder()
                .put("status", Status.UNAVAILABLE)
                .put("keyspace", "ks1")
                .put("replication", replication)
                .put("consistencyLevel", EACH_QUORUM)
                .put("availableRanges", 1)
                .put("unavailableRanges", 1)
                .put(
                    "top10UnavailableRanges",
                    ImmutableMap.of(
                        "]1,2]",
                        ImmutableMap.of(
                            "dc1",
                            ImmutableMap.of("alive", 1, "required", 2),
                            "dc2",
                            ImmutableMap.of("alive", 0, "required", 1))))
                .build());
  }
}
