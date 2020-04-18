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
import com.datastax.oss.driver.api.core.metadata.diagnostic.LocalTokenRingDiagnostic;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TokenRangeDiagnostic;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.Set;

public class DefaultLocalTokenRingDiagnostic extends DefaultTokenRingDiagnostic
    implements LocalTokenRingDiagnostic {

  private final String datacenter;

  public DefaultLocalTokenRingDiagnostic(
      @NonNull KeyspaceMetadata keyspace,
      @NonNull ConsistencyLevel consistencyLevel,
      @NonNull String datacenter,
      @NonNull Set<TokenRangeDiagnostic> tokenRangeDiagnostics) {
    super(keyspace, consistencyLevel, tokenRangeDiagnostics);
    Objects.requireNonNull(datacenter, "datacenter cannot be null");
    this.datacenter = datacenter;
  }

  @NonNull
  @Override
  public String getDatacenter() {
    return datacenter;
  }

  @Override
  protected void addBasicDetails(ImmutableMap.Builder<String, Object> details) {
    super.addBasicDetails(details);
    details.put("datacenter", datacenter);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LocalTokenRingDiagnostic)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    LocalTokenRingDiagnostic that = (LocalTokenRingDiagnostic) o;
    return getDatacenter().equals(that.getDatacenter());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), datacenter);
  }
}
