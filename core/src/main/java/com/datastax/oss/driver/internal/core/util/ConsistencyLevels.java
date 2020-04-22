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
package com.datastax.oss.driver.internal.core.util;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.internal.core.metadata.token.ReplicationFactor;
import edu.umd.cs.findbugs.annotations.NonNull;

public final class ConsistencyLevels {

  private ConsistencyLevels() {}

  /**
   * Determines the number of replicas required to achieve the desired {@linkplain ConsistencyLevel
   * consistency level} on a keyspace/datacenter with the given {@linkplain ReplicationFactor
   * replication factor}.
   *
   * @param consistencyLevel the {@linkplain ConsistencyLevel consistency level} to achieve.
   * @param replicationFactor the {@linkplain ReplicationFactor replication factor}.
   * @return the number of replicas required.
   */
  public static int requiredReplicas(
      @NonNull ConsistencyLevel consistencyLevel, @NonNull ReplicationFactor replicationFactor) {
    if (consistencyLevel instanceof DefaultConsistencyLevel) {
      DefaultConsistencyLevel defaultConsistencyLevel = (DefaultConsistencyLevel) consistencyLevel;
      switch (defaultConsistencyLevel) {
        case ANY:
        case ONE:
        case LOCAL_ONE:
          return 1;
        case TWO:
          return 2;
        case THREE:
          return 3;
        case QUORUM:
        case LOCAL_QUORUM:
        case EACH_QUORUM:
          return (replicationFactor.fullReplicas() / 2) + 1;
        case ALL:
          return replicationFactor.fullReplicas();
        default:
          // fall-through
      }
    }
    throw new IllegalArgumentException("Unsupported consistency level: " + consistencyLevel);
  }
}
