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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * Indicates that the load balancing policy has assigned a new distance to a host.
 *
 * <p>This is informational only: firing this event manually does <b>not</b> change the distance.
 */
@Immutable
public class DistanceEvent {
  public final NodeDistance distance;
  public final DefaultNode node;

  public DistanceEvent(NodeDistance distance, DefaultNode node) {
    this.distance = distance;
    this.node = node;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DistanceEvent) {
      DistanceEvent that = (DistanceEvent) other;
      return this.distance == that.distance
          && Objects.equals(this.node.getConnectAddress(), that.node.getConnectAddress());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.distance, this.node.getConnectAddress());
  }

  @Override
  public String toString() {
    return "DistanceEvent(" + distance + ", " + node.getConnectAddress() + ")";
  }
}
