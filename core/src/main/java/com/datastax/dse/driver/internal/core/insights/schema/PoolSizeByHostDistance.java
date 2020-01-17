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
package com.datastax.dse.driver.internal.core.insights.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class PoolSizeByHostDistance {
  @JsonProperty("local")
  private final int local;

  @JsonProperty("remote")
  private final int remote;

  @JsonProperty("ignored")
  private final int ignored;

  @JsonCreator
  public PoolSizeByHostDistance(
      @JsonProperty("local") int local,
      @JsonProperty("remote") int remote,
      @JsonProperty("ignored") int ignored) {

    this.local = local;
    this.remote = remote;
    this.ignored = ignored;
  }

  public int getLocal() {
    return local;
  }

  public int getRemote() {
    return remote;
  }

  public int getIgnored() {
    return ignored;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PoolSizeByHostDistance)) {
      return false;
    }
    PoolSizeByHostDistance that = (PoolSizeByHostDistance) o;
    return local == that.local && remote == that.remote && ignored == that.ignored;
  }

  @Override
  public int hashCode() {
    return Objects.hash(local, remote, ignored);
  }

  @Override
  public String toString() {
    return "PoolSizeByHostDistance{"
        + "local="
        + local
        + ", remote="
        + remote
        + ", ignored="
        + ignored
        + '}';
  }
}
