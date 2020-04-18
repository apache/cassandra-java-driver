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
package com.datastax.oss.driver.internal.core.metadata.diagnostic.topology;

import com.datastax.oss.driver.api.core.metadata.diagnostic.NodeGroupDiagnostic;
import java.util.Objects;

public class DefaultNodeGroupDiagnostic implements NodeGroupDiagnostic {

  private final int total;
  private final int up;
  private final int down;
  private final int unknown;

  public DefaultNodeGroupDiagnostic(int total, int up, int down, final int unknown) {
    this.total = total;
    this.up = up;
    this.down = down;
    this.unknown = unknown;
  }

  @Override
  public int getTotal() {
    return this.total;
  }

  @Override
  public int getUp() {
    return this.up;
  }

  @Override
  public int getDown() {
    return this.down;
  }

  @Override
  public int getUnknown() {
    return unknown;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DefaultNodeGroupDiagnostic)) {
      return false;
    }
    DefaultNodeGroupDiagnostic that = (DefaultNodeGroupDiagnostic) o;
    return total == that.total && up == that.up && down == that.down && unknown == that.unknown;
  }

  @Override
  public int hashCode() {
    return Objects.hash(total, up, down, unknown);
  }

  public static class Builder {

    private int total;
    private int up;
    private int down;
    private int unknown;

    public void incrementTotal() {
      this.total++;
    }

    public void incrementUp() {
      this.up++;
    }

    public void incrementDown() {
      this.down++;
    }

    public void incrementUnknown() {
      this.unknown++;
    }

    public DefaultNodeGroupDiagnostic build() {
      return new DefaultNodeGroupDiagnostic(total, up, down, unknown);
    }
  }
}
