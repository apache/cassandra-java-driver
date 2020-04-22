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
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultReplicationStrategyFactory;
import com.datastax.oss.driver.internal.core.metadata.token.ReplicationFactor;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

/** A factory for {@link TokenRingDiagnosticGenerator} instances. */
public class TokenRingDiagnosticGeneratorFactory {

  private final Metadata metadata;

  public TokenRingDiagnosticGeneratorFactory(@NonNull Metadata metadata) {
    Objects.requireNonNull(metadata, "metadata must not be null");
    this.metadata = metadata;
  }

  @NonNull
  public TokenRingDiagnosticGenerator create(
      @NonNull CqlIdentifier keyspaceName,
      @NonNull ConsistencyLevel consistencyLevel,
      @Nullable String datacenter) {
    Objects.requireNonNull(keyspaceName, "keyspaceName must not be null");
    Objects.requireNonNull(consistencyLevel, "consistencyLevel must not be null");
    if (!metadata.getTokenMap().isPresent()) {
      throw new IllegalArgumentException("Token metadata computation has been disabled");
    }
    Optional<KeyspaceMetadata> maybeKeyspace = metadata.getKeyspace(keyspaceName);
    if (!maybeKeyspace.isPresent()) {
      throw new IllegalArgumentException(
          String.format(
              "Keyspace %s does not exist or its metadata could not be retrieved",
              keyspaceName.asCql(true)));
    }
    KeyspaceMetadata keyspace = maybeKeyspace.get();
    String replicationStrategyClass = keyspace.getReplication().get("class");
    if (replicationStrategyClass.equals(DefaultReplicationStrategyFactory.SIMPLE_STRATEGY)) {
      return createForSimpleStrategy(keyspace, consistencyLevel);
    }
    if (replicationStrategyClass.equals(
        DefaultReplicationStrategyFactory.NETWORK_TOPOLOGY_STRATEGY)) {
      return createForNetworkTopologyStrategy(keyspace, consistencyLevel, datacenter);
    }
    throw new IllegalArgumentException(
        String.format(
            "Unsupported replication strategy '%s' for keyspace %s",
            replicationStrategyClass, keyspace.getName().asCql(true)));
  }

  protected TokenRingDiagnosticGenerator createForSimpleStrategy(
      KeyspaceMetadata keyspace, ConsistencyLevel consistencyLevel) {
    if (consistencyLevel.isDcLocal() || consistencyLevel == ConsistencyLevel.EACH_QUORUM) {
      throw new IllegalArgumentException(
          String.format(
              "Consistency level %s is not compatible with the SimpleStrategy replication configured for keyspace %s",
              consistencyLevel, keyspace.getName().asCql(true)));
    }
    ReplicationFactor replicationFactor =
        ReplicationFactor.fromString(keyspace.getReplication().get("replication_factor"));
    return new DefaultTokenRingDiagnosticGenerator(
        metadata, keyspace, consistencyLevel, replicationFactor);
  }

  protected TokenRingDiagnosticGenerator createForNetworkTopologyStrategy(
      KeyspaceMetadata keyspace, ConsistencyLevel consistencyLevel, String datacenter) {
    if (consistencyLevel.isDcLocal()) {
      return createForLocalCL(keyspace, consistencyLevel, datacenter);
    }
    if (consistencyLevel == ConsistencyLevel.EACH_QUORUM) {
      return createForEachQuorum(keyspace);
    }
    return createForNonLocalCL(keyspace, consistencyLevel);
  }

  protected TokenRingDiagnosticGenerator createForLocalCL(
      KeyspaceMetadata keyspace, ConsistencyLevel consistencyLevel, String datacenter) {
    if (datacenter == null) {
      // If the consistency level is local, we also need the local DC name
      throw new IllegalArgumentException(
          String.format(
              "No local datacenter was provided, but the consistency level is local (%s)",
              consistencyLevel));
    }
    Map<String, String> replication = keyspace.getReplication();
    if (!replication.containsKey(datacenter)) {
      // Bad config: the specified local DC is not listed in the replication options
      throw new IllegalArgumentException(
          String.format(
              "The local datacenter (%s) does not have a corresponding entry in replication options for keyspace %s",
              datacenter, keyspace.getName().asCql(true)));
    }
    ReplicationFactor replicationFactor = ReplicationFactor.fromString(replication.get(datacenter));
    return new LocalTokenRingDiagnosticGenerator(
        metadata, keyspace, consistencyLevel, datacenter, replicationFactor);
  }

  protected TokenRingDiagnosticGenerator createForEachQuorum(KeyspaceMetadata keyspace) {
    Map<String, ReplicationFactor> replicationFactorsByDc = new HashMap<>();
    for (Entry<String, String> entry : keyspace.getReplication().entrySet()) {
      if (entry.getKey().equals("class")) {
        continue;
      }
      String datacenter = entry.getKey();
      ReplicationFactor replicationFactor = ReplicationFactor.fromString(entry.getValue());
      replicationFactorsByDc.put(datacenter, replicationFactor);
    }
    return new EachQuorumTokenRingDiagnosticGenerator(metadata, keyspace, replicationFactorsByDc);
  }

  protected TokenRingDiagnosticGenerator createForNonLocalCL(
      KeyspaceMetadata keyspace, ConsistencyLevel consistencyLevel) {
    int sumOfReplicationFactors = 0;
    for (Entry<String, String> entry : keyspace.getReplication().entrySet()) {
      if (entry.getKey().equals("class")) {
        continue;
      }
      int replicationFactor = ReplicationFactor.fromString(entry.getValue()).fullReplicas();
      sumOfReplicationFactors += replicationFactor;
    }
    return new DefaultTokenRingDiagnosticGenerator(
        metadata, keyspace, consistencyLevel, new ReplicationFactor(sumOfReplicationFactors));
  }
}
