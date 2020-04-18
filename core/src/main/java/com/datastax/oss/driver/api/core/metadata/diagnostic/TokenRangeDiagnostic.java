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
package com.datastax.oss.driver.api.core.metadata.diagnostic;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

/**
 * A health {@link Diagnostic} for a given {@link TokenRange}, detailing whether or not the
 * configured {@linkplain #getConsistencyLevel() consistency level} can be achieved for the
 * configured {@linkplain #getKeyspace() keyspace} on it.
 */
public interface TokenRangeDiagnostic extends Diagnostic, Comparable<TokenRangeDiagnostic> {

  /** Returns the {@linkplain TokenRange token range} this report refers to. */
  @NonNull
  TokenRange getTokenRange();

  /** Returns the {@linkplain KeyspaceMetadata keyspace} this report refers to. */
  @NonNull
  KeyspaceMetadata getKeyspace();

  /** Returns the {@linkplain ConsistencyLevel consistency level} this report refers to. */
  @NonNull
  ConsistencyLevel getConsistencyLevel();

  /**
   * Whether this {@linkplain #getTokenRange() token range} is available or not.
   *
   * <p>A token range is considered available when the {@linkplain #getConsistencyLevel()
   * consistency level} is achievable on it. For that to happen, the number of alive replicas must
   * be equal to or greater than the number of required replicas.
   *
   * @return true if the token range is available, false otherwise.
   */
  default boolean isAvailable() {
    return getAliveReplicas() >= getRequiredReplicas();
  }

  /**
   * Returns how many replicas must be alive on this {@linkplain #getTokenRange() token range} for
   * it to be considered {@linkplain #isAvailable() available}.
   *
   * @return how many replicas must be alive.
   */
  int getRequiredReplicas();

  /**
   * Returns how many replicas are effectively alive on this {@linkplain #getTokenRange() token
   * range}.
   *
   * @return how many replicas are effectively alive.
   */
  int getAliveReplicas();

  @NonNull
  @Override
  default Status getStatus() {
    return isAvailable() ? Status.AVAILABLE : Status.UNAVAILABLE;
  }

  @NonNull
  @Override
  default Map<String, Object> getDetails() {
    return ImmutableMap.of("required", getRequiredReplicas(), "alive", getAliveReplicas());
  }

  @Override
  default int compareTo(TokenRangeDiagnostic that) {
    return this.getTokenRange().compareTo(that.getTokenRange());
  }
}
