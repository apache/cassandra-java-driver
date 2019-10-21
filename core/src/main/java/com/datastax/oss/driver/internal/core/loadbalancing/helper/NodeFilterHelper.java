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
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import net.jcip.annotations.ThreadSafe;

@FunctionalInterface
@ThreadSafe
public interface NodeFilterHelper {

  Predicate<Node> INCLUDE_ALL_NODES = n -> true;

  /**
   * @param localDc The local datacenter, or null if none defined.
   * @param nodes All the nodes that were known to exist in the cluster (regardless of their state)
   *     when the load balancing policy was {@linkplain LoadBalancingPolicy#init(Map,
   *     LoadBalancingPolicy.DistanceReporter) initialized}. This argument is provided in case
   *     implementors need to inspect the cluster topology to discover the local datacenter.
   * @return the node filter to use.
   */
  @NonNull
  Predicate<Node> createNodeFilter(@Nullable String localDc, @NonNull Map<UUID, Node> nodes);
}
