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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class SingleDcNodeSet implements NodeSet {

  private final Set<Node> nodes = new CopyOnWriteArraySet<>();

  private final String dc;
  private final Set<String> dcs;

  public SingleDcNodeSet(@NonNull String dc) {
    this.dc = dc;
    dcs = ImmutableSet.of(dc);
  }

  @Override
  public boolean add(@NonNull Node node) {
    if (Objects.equals(node.getDatacenter(), dc)) {
      return nodes.add(node);
    }
    return false;
  }

  @Override
  public boolean remove(@NonNull Node node) {
    if (Objects.equals(node.getDatacenter(), dc)) {
      return nodes.remove(node);
    }
    return false;
  }

  @Override
  @NonNull
  public Set<Node> dc(@Nullable String dc) {
    if (Objects.equals(this.dc, dc)) {
      return nodes;
    }
    return Collections.emptySet();
  }

  @Override
  public Set<String> dcs() {
    return dcs;
  }
}
