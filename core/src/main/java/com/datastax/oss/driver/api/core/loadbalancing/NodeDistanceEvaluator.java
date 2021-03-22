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
package com.datastax.oss.driver.api.core.loadbalancing;

import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A pluggable {@link NodeDistance} evaluator.
 *
 * <p>Node distance evaluators are recognized by all the driver built-in load balancing policies.
 * They can be specified {@linkplain
 * com.datastax.oss.driver.api.core.session.SessionBuilder#withNodeDistanceEvaluator(String,
 * NodeDistanceEvaluator) programmatically} or through the configuration (with the {@code
 * load-balancing-policy.evaluator.class} option).
 *
 * @see com.datastax.oss.driver.api.core.session.SessionBuilder#withNodeDistanceEvaluator(String,
 *     NodeDistanceEvaluator)
 */
@FunctionalInterface
public interface NodeDistanceEvaluator {

  /**
   * Evaluates the distance to apply to the given node.
   *
   * <p>This method will be invoked each time the {@link LoadBalancingPolicy} processes a topology
   * or state change, and will be passed the node being inspected, and the local datacenter name (or
   * null if none is defined). If it returns a non-null {@link NodeDistance}, the policy will
   * suggest that distance for the node; if it returns null, the policy will assign a default
   * distance instead, based on its internal algorithm for computing node distances.
   *
   * @param node The node to assign a new distance to.
   * @param localDc The local datacenter name, if defined, or null otherwise.
   * @return The {@link NodeDistance} to assign to the node, or null to let the policy decide.
   */
  @Nullable
  NodeDistance evaluateDistance(@NonNull Node node, @Nullable String localDc);
}
