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
import com.datastax.oss.driver.api.core.metadata.diagnostic.TokenRangeDiagnostic;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

/**
 * A {@link TokenRangeDiagnostic} that reports a single, global number of alive replicas.
 *
 * <p>This report is suitable for most replication strategies and {@linkplain ConsistencyLevel
 * consistency levels}, except {@link ConsistencyLevel#EACH_QUORUM}, for which it is recommended to
 * use {@link CompositeTokenRangeDiagnostic} instead.
 *
 * <p>It can be used either locally, at datacenter level, for consistency levels that are intended
 * to be {@linkplain ConsistencyLevel#isDcLocal() achieved locally}, or either globally, for
 * SimpleStrategy replications and non-local consistency levels.
 */
public class SimpleTokenRangeDiagnostic implements TokenRangeDiagnostic {

  private final TokenRange range;

  private final KeyspaceMetadata keyspace;

  private final ConsistencyLevel consistencyLevel;

  private final int requiredReplicas;

  private final int aliveReplicas;

  public SimpleTokenRangeDiagnostic(
      @NonNull TokenRange range,
      @NonNull KeyspaceMetadata keyspace,
      @NonNull ConsistencyLevel consistencyLevel,
      int requiredReplicas,
      int aliveReplicas) {
    Objects.requireNonNull(range, "range cannot be null");
    Objects.requireNonNull(keyspace, "keyspace cannot be null");
    Objects.requireNonNull(consistencyLevel, "consistencyLevel cannot be null");
    this.range = range;
    this.keyspace = keyspace;
    this.consistencyLevel = consistencyLevel;
    this.requiredReplicas = requiredReplicas;
    this.aliveReplicas = aliveReplicas;
  }

  @NonNull
  @Override
  public TokenRange getTokenRange() {
    return range;
  }

  @NonNull
  @Override
  public KeyspaceMetadata getKeyspace() {
    return keyspace;
  }

  @NonNull
  @Override
  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  @Override
  public int getRequiredReplicas() {
    return requiredReplicas;
  }

  @Override
  public int getAliveReplicas() {
    return aliveReplicas;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SimpleTokenRangeDiagnostic)) {
      return false;
    }
    SimpleTokenRangeDiagnostic that = (SimpleTokenRangeDiagnostic) o;
    return requiredReplicas == that.requiredReplicas
        && aliveReplicas == that.aliveReplicas
        && range.equals(that.range)
        && keyspace.equals(that.keyspace)
        && consistencyLevel.equals(that.consistencyLevel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(range, keyspace, consistencyLevel, requiredReplicas, aliveReplicas);
  }
}
