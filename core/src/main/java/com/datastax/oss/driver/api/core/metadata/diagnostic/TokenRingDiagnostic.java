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
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;
import java.util.SortedSet;

/**
 * A health {@link Diagnostic} on the availability of {@linkplain TokenRange token ranges} in the
 * token ring, for a given {@linkplain #getKeyspace() keyspace} and a given {@linkplain
 * #getConsistencyLevel() consistency level}.
 */
public interface TokenRingDiagnostic extends Diagnostic {

  /** Returns the {@linkplain KeyspaceMetadata keyspace} this report refers to. */
  @NonNull
  KeyspaceMetadata getKeyspace();

  /**
   * Returns the {@linkplain ConsistencyLevel consistency level} that was used to create this
   * diagnostic.
   */
  @NonNull
  ConsistencyLevel getConsistencyLevel();

  /**
   * If this diagnostic was produced for a {@linkplain ConsistencyLevel#isDcLocal()
   * datacenter-local} consistency level, returns the datacenter for which this diagnostic was
   * established. For all other consistency levels, returns {@link Optional#empty()}.
   */
  Optional<String> getDatacenter();

  /**
   * @return the {@link TokenRangeDiagnostic}s for each individual {@linkplain TokenRange token
   *     range}.
   */
  @NonNull
  SortedSet<TokenRangeDiagnostic> getTokenRangeDiagnostics();

  /**
   * Returns the {@link Status} of the entire ring.
   *
   * <p>The ring is considered available when all its {@linkplain TokenRange token ranges} are
   * {@linkplain TokenRangeDiagnostic#isAvailable() available}.
   */
  @NonNull
  @Override
  default Status getStatus() {
    return getTokenRangeDiagnostics().stream()
        .map(TokenRangeDiagnostic::getStatus)
        .reduce(Status::mergeWith)
        .orElse(Status.UNKNOWN);
  }
}
