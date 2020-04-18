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
import com.datastax.oss.driver.api.core.metadata.diagnostic.Status;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TokenRingDiagnostic;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A component that checks the health of a Cassandra token ring, and reports if a configured
 * consistency level, on a configured keyspace, is achievable or not for each token range in the
 * ring.
 */
public interface TokenRingDiagnosticGenerator {

  /**
   * Generates a health diagnostic for the token ring.
   *
   * <p>The token ring should be considered healthy if all {@linkplain TokenRange token ranges} are
   * available; a token range is considered available if the configured {@linkplain ConsistencyLevel
   * consistency level} is achievable on it for the configured {@linkplain KeyspaceMetadata
   * keyspace}.
   *
   * <p>If this condition is not met, this reporter should set the health status to {@link
   * Status#UNAVAILABLE}, as this means that some queries will result in {@link
   * UnavailableException} errors.
   */
  @NonNull
  TokenRingDiagnostic generate();
}
