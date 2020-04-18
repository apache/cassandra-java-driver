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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultReplicationStrategyFactory;
import com.datastax.oss.driver.internal.core.metadata.token.ReplicationFactor;
import com.datastax.oss.driver.internal.core.util.ConsistencyLevels;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** A factory for {@link TokenRingDiagnosticGenerator} instances. */
public class TokenRingDiagnosticGeneratorFactory {

  private static final Log LOG = LogFactory.getLog(TokenRingDiagnosticGeneratorFactory.class);

  private final Metadata metadata;

  public TokenRingDiagnosticGeneratorFactory(@NonNull Metadata metadata) {
    Objects.requireNonNull(metadata, "metadata must not be null");
    this.metadata = metadata;
  }

  @NonNull
  public Optional<TokenRingDiagnosticGenerator> maybeCreate(
      @NonNull KeyspaceMetadata keyspace,
      @NonNull ConsistencyLevel consistencyLevel,
      @Nullable String datacenter) {
    Objects.requireNonNull(keyspace, "keyspace must not be null");
    Objects.requireNonNull(consistencyLevel, "consistencyLevel must not be null");
    if (!metadata.getTokenMap().isPresent()) {
      LOG.warn(
          "Token metadata computation has been disabled. Token ring health reports will be disabled");
      return Optional.empty();
    }
    String replicationStrategyClass = keyspace.getReplication().get("class");
    if (replicationStrategyClass.equals(DefaultReplicationStrategyFactory.SIMPLE_STRATEGY)) {
      return createForSimpleStrategy(keyspace, consistencyLevel);
    }
    if (replicationStrategyClass.equals(
        DefaultReplicationStrategyFactory.NETWORK_TOPOLOGY_STRATEGY)) {
      return createForNetworkTopologyStrategy(keyspace, consistencyLevel, datacenter);
    }
    LOG.warn(
        String.format(
            "Unsupported replication strategy '%s' for keyspace %s. Token ring health reports will be disabled",
            replicationStrategyClass, keyspace.getName().asCql(true)));
    return Optional.empty();
  }

  protected Optional<TokenRingDiagnosticGenerator> createForSimpleStrategy(
      KeyspaceMetadata keyspace, ConsistencyLevel consistencyLevel) {
    ConsistencyLevel filteredConsistencyLevel =
        ConsistencyLevels.filterForSimpleStrategy(consistencyLevel);
    if (filteredConsistencyLevel != consistencyLevel) {
      LOG.warn(
          String.format(
              "Consistency level %s is not compatible with the SimpleStrategy replication configured for keyspace %s."
                  + "Token ring health reports will assume %s instead.",
              consistencyLevel, keyspace.getName().asCql(true), filteredConsistencyLevel));
    }
    ReplicationFactor replicationFactor =
        ReplicationFactor.fromString(keyspace.getReplication().get("replication_factor"));
    DefaultTokenRingDiagnosticGenerator generator =
        new DefaultTokenRingDiagnosticGenerator(
            metadata, keyspace, filteredConsistencyLevel, replicationFactor);
    return Optional.of(generator);
  }

  protected Optional<TokenRingDiagnosticGenerator> createForNetworkTopologyStrategy(
      KeyspaceMetadata keyspace, ConsistencyLevel consistencyLevel, String datacenter) {
    if (consistencyLevel.isDcLocal()) {
      return createForLocalCL(keyspace, consistencyLevel, datacenter);
    }
    if (consistencyLevel == ConsistencyLevel.EACH_QUORUM) {
      return createForEachQuorum(keyspace);
    }
    return createForNonLocalCL(keyspace, consistencyLevel);
  }

  protected Optional<TokenRingDiagnosticGenerator> createForLocalCL(
      KeyspaceMetadata keyspace, ConsistencyLevel consistencyLevel, String datacenter) {
    if (datacenter == null) {
      // If the consistency level is local, we also need the local DC name
      LOG.warn(
          String.format(
              "No local datacenter was provided, but the consistency level is local (%s). Token ring health reports will be disabled",
              consistencyLevel));
      return Optional.empty();
    }
    Map<String, String> replication = keyspace.getReplication();
    if (!replication.containsKey(datacenter)) {
      // Bad config: the specified local DC is not listed in the replication options
      LOG.warn(
          String.format(
              "The local datacenter (%s) does not have a corresponding entry in replication options for keyspace %s. Token ring health reports will be disabled",
              datacenter, keyspace.getName().asCql(true)));
      return Optional.empty();
    }
    ReplicationFactor replicationFactor = ReplicationFactor.fromString(replication.get(datacenter));
    LocalTokenRingDiagnosticGenerator generator =
        new LocalTokenRingDiagnosticGenerator(
            metadata, keyspace, consistencyLevel, datacenter, replicationFactor);
    return Optional.of(generator);
  }

  protected Optional<TokenRingDiagnosticGenerator> createForEachQuorum(KeyspaceMetadata keyspace) {
    Map<String, ReplicationFactor> replicationFactorsByDc = new HashMap<>();
    for (Entry<String, String> entry : keyspace.getReplication().entrySet()) {
      if (entry.getKey().equals("class")) {
        continue;
      }
      String datacenter = entry.getKey();
      ReplicationFactor replicationFactor = ReplicationFactor.fromString(entry.getValue());
      replicationFactorsByDc.put(datacenter, replicationFactor);
    }
    EachQuorumTokenRingDiagnosticGenerator generator =
        new EachQuorumTokenRingDiagnosticGenerator(metadata, keyspace, replicationFactorsByDc);
    return Optional.of(generator);
  }

  protected Optional<TokenRingDiagnosticGenerator> createForNonLocalCL(
      KeyspaceMetadata keyspace, ConsistencyLevel consistencyLevel) {
    int sumOfReplicationFactors = 0;
    for (Entry<String, String> entry : keyspace.getReplication().entrySet()) {
      if (entry.getKey().equals("class")) {
        continue;
      }
      int replicationFactor = ReplicationFactor.fromString(entry.getValue()).fullReplicas();
      sumOfReplicationFactors += replicationFactor;
    }
    DefaultTokenRingDiagnosticGenerator generator =
        new DefaultTokenRingDiagnosticGenerator(
            metadata, keyspace, consistencyLevel, new ReplicationFactor(sumOfReplicationFactors));
    return Optional.of(generator);
  }
}
