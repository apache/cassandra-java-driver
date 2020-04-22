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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.BDDMockito.given;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultReplicationStrategyFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TokenRingDiagnosticGeneratorFactoryTest {

  @Mock Metadata metadata;
  @Mock TokenMap tokenMap;

  @Mock(name = "ks1")
  KeyspaceMetadata ks;

  CqlIdentifier ksName = CqlIdentifier.fromInternal("ks1");
  Map<String, String> replication =
      ImmutableMap.of(
          "class",
          DefaultReplicationStrategyFactory.NETWORK_TOPOLOGY_STRATEGY,
          "dc1",
          "3",
          "dc2",
          "3");

  @Before
  public void setUp() {
    given(metadata.getKeyspace(ksName)).willReturn(Optional.of(ks));
    given(ks.getName()).willReturn(ksName);
    given(ks.getReplication()).willReturn(replication);
    given(metadata.getTokenMap()).willReturn(Optional.of(tokenMap));
  }

  @Test
  public void should_create_generator_for_simple_strategy_and_non_local_CL() {
    // given
    given(ks.getReplication())
        .willReturn(
            ImmutableMap.of(
                "class",
                DefaultReplicationStrategyFactory.SIMPLE_STRATEGY,
                "replication_factor",
                "3"));
    // when
    TokenRingDiagnosticGeneratorFactory factory = new TokenRingDiagnosticGeneratorFactory(metadata);
    // then
    TokenRingDiagnosticGenerator generator = factory.create(ksName, ConsistencyLevel.QUORUM, null);
    assertThat(generator)
        .isExactlyInstanceOf(DefaultTokenRingDiagnosticGenerator.class)
        .extracting("consistencyLevel", "requiredReplicas")
        .containsExactly(ConsistencyLevel.QUORUM, 2);
  }

  @Test
  public void should_not_create_generator_for_simple_strategy_and_incompatible_CL() {
    // given
    given(ks.getReplication())
        .willReturn(
            com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap.of(
                "class",
                DefaultReplicationStrategyFactory.SIMPLE_STRATEGY,
                "replication_factor",
                "3"));
    // when
    TokenRingDiagnosticGeneratorFactory factory = new TokenRingDiagnosticGeneratorFactory(metadata);
    // then
    Throwable throwable =
        catchThrowable(() -> factory.create(ksName, ConsistencyLevel.LOCAL_QUORUM, "dc1"));
    assertThat(throwable)
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Consistency level LOCAL_QUORUM is not compatible with the SimpleStrategy");
  }

  @Test
  public void should_create_generator_for_network_topology_strategy_and_local_CL() {
    // given
    given(ks.getReplication())
        .willReturn(
            com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap.of(
                "class",
                DefaultReplicationStrategyFactory.NETWORK_TOPOLOGY_STRATEGY,
                "dc1",
                "3",
                "dc2",
                "1"));
    // when
    TokenRingDiagnosticGeneratorFactory factory = new TokenRingDiagnosticGeneratorFactory(metadata);
    // then
    TokenRingDiagnosticGenerator generator =
        factory.create(ksName, ConsistencyLevel.LOCAL_QUORUM, "dc1");
    assertThat(generator)
        .isExactlyInstanceOf(LocalTokenRingDiagnosticGenerator.class)
        .extracting("consistencyLevel", "requiredReplicas", "datacenter")
        .containsExactly(ConsistencyLevel.LOCAL_QUORUM, 2, "dc1");
  }

  @Test
  public void
      should_not_create_generator_for_network_topology_strategy_when_local_CL_and_no_local_dc() {
    // given
    given(ks.getReplication())
        .willReturn(
            com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap.of(
                "class",
                DefaultReplicationStrategyFactory.NETWORK_TOPOLOGY_STRATEGY,
                "dc1",
                "3",
                "dc2",
                "1"));
    // when
    TokenRingDiagnosticGeneratorFactory factory = new TokenRingDiagnosticGeneratorFactory(metadata);
    // then
    Throwable throwable =
        catchThrowable(() -> factory.create(ksName, ConsistencyLevel.LOCAL_QUORUM, null));
    assertThat(throwable)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "No local datacenter was provided, but the consistency level is local (LOCAL_QUORUM)");
  }

  @Test
  public void
      should_not_create_generator_for_network_topology_strategy_when_local_CL_and_dc_not_found() {
    // given
    given(ks.getReplication())
        .willReturn(
            com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap.of(
                "class",
                DefaultReplicationStrategyFactory.NETWORK_TOPOLOGY_STRATEGY,
                "dc1",
                "3",
                "dc2",
                "1"));
    // when
    TokenRingDiagnosticGeneratorFactory factory = new TokenRingDiagnosticGeneratorFactory(metadata);
    // then
    Throwable throwable =
        catchThrowable(() -> factory.create(ksName, ConsistencyLevel.LOCAL_QUORUM, "dc3"));
    assertThat(throwable)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "The local datacenter (dc3) does not have a corresponding entry in replication options for keyspace ks1");
  }

  @Test
  public void should_create_generator_for_network_topology_strategy_and_non_local_CL() {
    // given
    given(ks.getReplication())
        .willReturn(
            com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap.of(
                "class",
                DefaultReplicationStrategyFactory.NETWORK_TOPOLOGY_STRATEGY,
                "dc1",
                "3",
                "dc2",
                "1"));
    // when
    TokenRingDiagnosticGeneratorFactory factory = new TokenRingDiagnosticGeneratorFactory(metadata);
    // then
    TokenRingDiagnosticGenerator generator = factory.create(ksName, ConsistencyLevel.QUORUM, null);
    assertThat(generator)
        .isExactlyInstanceOf(DefaultTokenRingDiagnosticGenerator.class)
        .extracting("consistencyLevel", "requiredReplicas")
        .containsExactly(ConsistencyLevel.QUORUM, 2 + 1);
  }

  @Test
  public void should_create_generator_for_network_topology_strategy_and_EACH_QUORUM() {
    // given
    given(ks.getReplication())
        .willReturn(
            com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap.of(
                "class",
                DefaultReplicationStrategyFactory.NETWORK_TOPOLOGY_STRATEGY,
                "dc1",
                "3",
                "dc2",
                "1"));
    // when
    TokenRingDiagnosticGeneratorFactory factory = new TokenRingDiagnosticGeneratorFactory(metadata);
    // then
    TokenRingDiagnosticGenerator generator =
        factory.create(ksName, ConsistencyLevel.EACH_QUORUM, null);
    assertThat(generator)
        .isExactlyInstanceOf(EachQuorumTokenRingDiagnosticGenerator.class)
        .extracting("requiredReplicasByDc")
        .isEqualTo(ImmutableMap.of("dc1", 2, "dc2", 1));
  }

  @Test
  public void should_not_create_generator_when_token_map_disabled() {
    // given
    given(metadata.getTokenMap()).willReturn(Optional.empty());
    // when
    TokenRingDiagnosticGeneratorFactory factory = new TokenRingDiagnosticGeneratorFactory(metadata);
    // then
    Throwable throwable =
        catchThrowable(() -> factory.create(ksName, ConsistencyLevel.QUORUM, null));
    assertThat(throwable)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Token metadata computation has been disabled");
  }

  @Test
  public void should_not_create_generator_when_keyspace_not_found() {
    // given
    given(metadata.getKeyspace(ksName)).willReturn(Optional.empty());
    // when
    TokenRingDiagnosticGeneratorFactory factory = new TokenRingDiagnosticGeneratorFactory(metadata);
    // then
    Throwable throwable =
        catchThrowable(() -> factory.create(ksName, ConsistencyLevel.QUORUM, null));
    assertThat(throwable)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Keyspace ks1 does not exist or its metadata could not be retrieved");
  }

  @Test
  public void should_not_create_generator_when_unsupported_strategy() {
    // given
    given(ks.getReplication())
        .willReturn(
            ImmutableMap.of("class", DefaultReplicationStrategyFactory.EVERYWHERE_STRATEGY));
    // when
    TokenRingDiagnosticGeneratorFactory factory = new TokenRingDiagnosticGeneratorFactory(metadata);
    // then
    Throwable throwable =
        catchThrowable(() -> factory.create(ksName, ConsistencyLevel.QUORUM, null));
    assertThat(throwable)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Unsupported replication strategy 'org.apache.cassandra.locator.EverywhereStrategy' for keyspace ks1");
  }
}
