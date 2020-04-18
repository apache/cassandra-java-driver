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
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Similar to a {@link TokenRingDiagnostic}, but the {@linkplain TokenRange token range}
 * availability checks are performed on a single {@linkplain #getDatacenter() datacenter} only.
 *
 * <p>This diagnostic is produced for all {@linkplain #getConsistencyLevel() consistency levels}
 * that are {@linkplain ConsistencyLevel#isDcLocal() datacenter-local}.
 */
public interface LocalTokenRingDiagnostic extends TokenRingDiagnostic {

  /** Returns that datacenter name for which this diagnostic was established. */
  @NonNull
  String getDatacenter();
}
