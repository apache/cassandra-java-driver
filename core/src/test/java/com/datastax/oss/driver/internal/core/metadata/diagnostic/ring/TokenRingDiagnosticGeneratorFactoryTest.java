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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultReplicationStrategyFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

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

  Logger logger;
  Level initialLogLevel;

  @Mock Appender<ILoggingEvent> appender;
  @Captor ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  @Before
  public void setUp() {
    given(metadata.getKeyspace(ksName)).willReturn(Optional.of(ks));
    given(ks.getName()).willReturn(ksName);
    given(ks.getReplication()).willReturn(replication);
    given(metadata.getTokenMap()).willReturn(Optional.of(tokenMap));
  }

  @Before
  public void addAppender() {
    logger = (Logger) LoggerFactory.getLogger(TokenRingDiagnosticGeneratorFactory.class);
    initialLogLevel = logger.getLevel();
    logger.setLevel(Level.WARN);
    logger.addAppender(appender);
  }

  @After
  public void removeAppender() {
    logger.detachAppender(appender);
    logger.setLevel(initialLogLevel);
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
    Optional<TokenRingDiagnosticGenerator> maybeGenerator =
        factory.maybeCreate(ksName, ConsistencyLevel.QUORUM, null);
    assertThat(maybeGenerator).isPresent();
    assertThat(maybeGenerator.get())
        .isExactlyInstanceOf(DefaultTokenRingDiagnosticGenerator.class)
        .extracting("consistencyLevel", "requiredReplicas")
        .containsExactly(ConsistencyLevel.QUORUM, 2);
    assertNoLog();
  }

  @Test
  public void should_create_generator_for_simple_strategy_and_filter_incompatible_CL() {
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
    Optional<TokenRingDiagnosticGenerator> maybeGenerator =
        factory.maybeCreate(ksName, ConsistencyLevel.LOCAL_QUORUM, "dc1");
    assertThat(maybeGenerator).isPresent();
    assertThat(maybeGenerator.get())
        .isExactlyInstanceOf(DefaultTokenRingDiagnosticGenerator.class)
        .extracting("consistencyLevel", "requiredReplicas")
        .containsExactly(ConsistencyLevel.QUORUM, 2);
    assertLog("Consistency level LOCAL_QUORUM is not compatible with the SimpleStrategy");
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
    Optional<TokenRingDiagnosticGenerator> maybeGenerator =
        factory.maybeCreate(ksName, ConsistencyLevel.LOCAL_QUORUM, "dc1");
    assertThat(maybeGenerator).isPresent();
    assertThat(maybeGenerator.get())
        .isExactlyInstanceOf(LocalTokenRingDiagnosticGenerator.class)
        .extracting("consistencyLevel", "requiredReplicas", "datacenter")
        .containsExactly(ConsistencyLevel.LOCAL_QUORUM, 2, "dc1");
    assertNoLog();
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
    Optional<TokenRingDiagnosticGenerator> maybeGenerator =
        factory.maybeCreate(ksName, ConsistencyLevel.LOCAL_QUORUM, null);
    assertThat(maybeGenerator).isNotPresent();
    assertLog(
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
    Optional<TokenRingDiagnosticGenerator> maybeGenerator =
        factory.maybeCreate(ksName, ConsistencyLevel.LOCAL_QUORUM, "dc3");
    assertThat(maybeGenerator).isNotPresent();
    assertLog(
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
    Optional<TokenRingDiagnosticGenerator> maybeGenerator =
        factory.maybeCreate(ksName, ConsistencyLevel.QUORUM, null);
    assertThat(maybeGenerator).isPresent();
    assertThat(maybeGenerator.get())
        .isExactlyInstanceOf(DefaultTokenRingDiagnosticGenerator.class)
        .extracting("consistencyLevel", "requiredReplicas")
        .containsExactly(ConsistencyLevel.QUORUM, 2 + 1);
    assertNoLog();
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
    Optional<TokenRingDiagnosticGenerator> maybeGenerator =
        factory.maybeCreate(ksName, ConsistencyLevel.EACH_QUORUM, null);
    assertThat(maybeGenerator).isPresent();
    assertThat(maybeGenerator.get())
        .isExactlyInstanceOf(EachQuorumTokenRingDiagnosticGenerator.class)
        .extracting("requiredReplicasByDc")
        .isEqualTo(ImmutableMap.of("dc1", 2, "dc2", 1));
    assertNoLog();
  }

  @Test
  public void should_not_create_generator_when_token_map_disabled() {
    // given
    given(metadata.getTokenMap()).willReturn(Optional.empty());
    // when
    TokenRingDiagnosticGeneratorFactory factory = new TokenRingDiagnosticGeneratorFactory(metadata);
    // then
    Optional<TokenRingDiagnosticGenerator> maybeGenerator =
        factory.maybeCreate(ksName, ConsistencyLevel.QUORUM, null);
    assertThat(maybeGenerator).isNotPresent();
    assertLog("Token metadata computation has been disabled");
  }

  @Test
  public void should_not_create_generator_when_keyspace_not_found() {
    // given
    given(metadata.getKeyspace(ksName)).willReturn(Optional.empty());
    // when
    TokenRingDiagnosticGeneratorFactory factory = new TokenRingDiagnosticGeneratorFactory(metadata);
    // then
    Optional<TokenRingDiagnosticGenerator> maybeGenerator =
        factory.maybeCreate(ksName, ConsistencyLevel.QUORUM, null);
    assertThat(maybeGenerator).isNotPresent();
    assertLog("Keyspace ks1 does not exist or its metadata could not be retrieved");
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
    Optional<TokenRingDiagnosticGenerator> maybeGenerator =
        factory.maybeCreate(ksName, ConsistencyLevel.QUORUM, null);
    assertThat(maybeGenerator).isNotPresent();
    assertLog(
        "Unsupported replication strategy 'org.apache.cassandra.locator.EverywhereStrategy' for keyspace ks1");
  }

  private void assertNoLog() {
    verify(appender, never()).doAppend(any());
  }

  private void assertLog(String message) {
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getFormattedMessage()).contains(message);
  }
}
