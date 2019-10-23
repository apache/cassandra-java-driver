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
package com.datastax.oss.driver.internal.core.loadbalancing.helper;

import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;

@FunctionalInterface
@ThreadSafe
public interface LocalDcHelper {

  /**
   * Returns the local datacenter, if it can be discovered, or returns {@link Optional#empty empty}
   * otherwise.
   *
   * <p>Implementors may choose to throw {@link IllegalStateException} instead of returning {@link
   * Optional#empty empty}, if they require a local datacenter to be defined in order to operate
   * properly.
   *
   * @param nodes All the nodes that were known to exist in the cluster (regardless of their state)
   *     when the load balancing policy was {@linkplain LoadBalancingPolicy#init(Map,
   *     LoadBalancingPolicy.DistanceReporter) initialized}. This argument is provided in case
   *     implementors need to inspect the cluster topology to discover the local datacenter.
   * @return The local datacenter, or {@link Optional#empty empty} if none found.
   * @throws IllegalStateException if the local datacenter could not be discovered, and this policy
   *     cannot operate without it.
   */
  @NonNull
  Optional<String> discoverLocalDc(@NonNull Map<UUID, Node> nodes);
}
