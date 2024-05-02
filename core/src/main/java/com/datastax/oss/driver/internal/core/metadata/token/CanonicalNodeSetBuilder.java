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
package com.datastax.oss.driver.internal.core.metadata.token;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jcip.annotations.NotThreadSafe;

/**
 * A reusable set builder that guarantees that identical sets (same elements in the same order) will
 * be represented by the same instance.
 */
@NotThreadSafe
class CanonicalNodeSetBuilder {

  private final Map<List<Node>, Set<Node>> canonicalSets = new HashMap<>();
  private final List<Node> elements = new ArrayList<>();

  void add(Node node) {
    // This is O(n), but the cardinality is low (max possible size is the replication factor).
    if (!elements.contains(node)) {
      elements.add(node);
    }
  }

  int size() {
    return elements.size();
  }

  Set<Node> build() {
    return canonicalSets.computeIfAbsent(elements, ImmutableSet::copyOf);
  }

  void clear() {
    elements.clear();
  }
}
