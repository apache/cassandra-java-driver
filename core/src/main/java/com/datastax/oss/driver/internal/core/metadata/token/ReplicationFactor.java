package com.datastax.oss.driver.internal.core.metadata.token;
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
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import java.util.List;
import java.util.Objects;

// This class is a subset of server version at org.apache.cassandra.locator.ReplicationFactor
public class ReplicationFactor {
  private final int allReplicas;
  private final int fullReplicas;

  private ReplicationFactor(int allReplicas, int transientReplicas) {
    this.allReplicas = allReplicas;
    this.fullReplicas = allReplicas - transientReplicas;
  }

  public static ReplicationFactor withTransient(int allReplicas, int transientReplicas) {
    return new ReplicationFactor(allReplicas, transientReplicas);
  }

  public static ReplicationFactor fullOnly(int allReplicas) {
    return new ReplicationFactor(allReplicas, 0);
  }

  public int fullReplicas() {
    return fullReplicas;
  }

  public int transientReplicas() {
    return allReplicas - fullReplicas;
  }

  public boolean hasTransientReplicas() {
    return allReplicas != fullReplicas;
  }

  public static ReplicationFactor fromString(String s) {
    if (s.contains("/")) {
      List<String> parts = Splitter.on('/').splitToList(s);
      Preconditions.checkArgument(
          parts.size() == 2, "Replication factor format is <replicas> or <replicas>/<transient>");
      return new ReplicationFactor(Integer.parseInt(parts.get(0)), Integer.parseInt(parts.get(1)));
    } else {
      return new ReplicationFactor(Integer.parseInt(s), 0);
    }
  }

  public String toParseableString() {
    return allReplicas + (hasTransientReplicas() ? "/" + transientReplicas() : "");
  }

  @Override
  public String toString() {
    return "rf(" + toParseableString() + ')';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ReplicationFactor)) return false;
    ReplicationFactor that = (ReplicationFactor) o;
    return allReplicas == that.allReplicas && fullReplicas == that.fullReplicas;
  }

  @Override
  public int hashCode() {
    return Objects.hash(allReplicas, fullReplicas);
  }
}
