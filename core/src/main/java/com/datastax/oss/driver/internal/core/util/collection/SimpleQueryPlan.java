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
package com.datastax.oss.driver.internal.core.util.collection;

import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.ThreadSafe;

/** Query plan where nodes must be provided at construction time. */
@ThreadSafe
public class SimpleQueryPlan extends QueryPlanBase {

  private final Object[] nodes;

  /**
   * Creates a new query plan with the given nodes.
   *
   * <p>For efficiency, there is no defensive copy, the provided array is used directly. The
   * declared type is {@code Object[]} but all elements must be instances of {@link Node}. See
   * {@link #getNodes()} for details.
   *
   * @param nodes the nodes to initially fill the queue with.
   */
  public SimpleQueryPlan(@NonNull Object... nodes) {
    this.nodes = nodes;
  }

  @Override
  protected Object[] getNodes() {
    return nodes;
  }
}
