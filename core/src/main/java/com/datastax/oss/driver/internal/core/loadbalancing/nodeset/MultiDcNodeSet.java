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
package com.datastax.oss.driver.internal.core.loadbalancing.nodeset;

import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class MultiDcNodeSet implements NodeSet {

  private static final String UNKNOWN_DC = "";

  private final Map<String, Set<Node>> nodes = new ConcurrentHashMap<>();

  @Override
  public boolean add(@NonNull Node node) {
    AtomicBoolean added = new AtomicBoolean();
    nodes.compute(
        getMapKey(node),
        (key, current) -> {
          if (current == null) {
            // We use CopyOnWriteArraySet because we need
            // 1) to preserve insertion order, and
            // 2) a "snapshot"-style toArray() implementation
            current = new CopyOnWriteArraySet<>();
          }
          if (current.add(node)) {
            added.set(true);
          }
          return current;
        });
    return added.get();
  }

  @Override
  public boolean remove(@NonNull Node node) {
    AtomicBoolean removed = new AtomicBoolean();
    nodes.compute(
        getMapKey(node),
        (key, current) -> {
          if (current != null) {
            if (current.remove(node)) {
              removed.set(true);
            }
          }
          return current;
        });
    return removed.get();
  }

  @Override
  @NonNull
  public Set<Node> dc(@Nullable String dc) {
    return nodes.getOrDefault(getMapKey(dc), Collections.emptySet());
  }

  @Override
  public Set<String> dcs() {
    return nodes.keySet();
  }

  @NonNull
  private String getMapKey(@NonNull Node node) {
    return getMapKey(node.getDatacenter());
  }

  @NonNull
  private String getMapKey(@Nullable String dc) {
    return dc == null ? UNKNOWN_DC : dc;
  }
}
