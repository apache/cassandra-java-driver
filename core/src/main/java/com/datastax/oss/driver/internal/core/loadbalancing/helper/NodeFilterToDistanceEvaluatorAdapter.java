/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.loadbalancing.helper;

import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator;
import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.function.Predicate;

public class NodeFilterToDistanceEvaluatorAdapter implements NodeDistanceEvaluator {

  private final Predicate<Node> nodeFilter;

  public NodeFilterToDistanceEvaluatorAdapter(@NonNull Predicate<Node> nodeFilter) {
    this.nodeFilter = nodeFilter;
  }

  @Nullable
  @Override
  public NodeDistance evaluateDistance(@NonNull Node node, @Nullable String localDc) {
    return nodeFilter.test(node) ? null : NodeDistance.IGNORED;
  }
}
