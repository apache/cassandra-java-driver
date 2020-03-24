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
import java.util.Objects;

// This class is a subset of server version at org.apache.cassandra.locator.ReplicationFactor
public class ReplicationFactor {
  private final int allReplicas;
  private final int fullReplicas;
  private final int transientReplicas;

  public ReplicationFactor(int allReplicas, int transientReplicas) {
    this.allReplicas = allReplicas;
    this.transientReplicas = transientReplicas;
    this.fullReplicas = allReplicas - transientReplicas;
  }

  public ReplicationFactor(int allReplicas) {
    this(allReplicas, 0);
  }

  public int fullReplicas() {
    return fullReplicas;
  }

  public int transientReplicas() {
    return transientReplicas;
  }

  public boolean hasTransientReplicas() {
    return allReplicas != fullReplicas;
  }

  public static ReplicationFactor fromString(String s) {
    if (s.contains("/")) {

      int slash = s.indexOf('/');
      String allPart = s.substring(0, slash);
      String transientPart = s.substring(slash + 1);
      return new ReplicationFactor(Integer.parseInt(allPart), Integer.parseInt(transientPart));
    } else {
      return new ReplicationFactor(Integer.parseInt(s), 0);
    }
  }

  @Override
  public String toString() {
    return allReplicas + (hasTransientReplicas() ? "/" + transientReplicas() : "");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ReplicationFactor)) {
      return false;
    }
    ReplicationFactor that = (ReplicationFactor) o;
    return allReplicas == that.allReplicas && fullReplicas == that.fullReplicas;
  }

  @Override
  public int hashCode() {
    return Objects.hash(allReplicas, fullReplicas);
  }
}
