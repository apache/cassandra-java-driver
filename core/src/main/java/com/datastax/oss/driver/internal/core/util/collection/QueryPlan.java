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

import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterators;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.ThreadSafe;

/**
 * A specialized, thread-safe queue implementation for {@link
 * LoadBalancingPolicy#newQueryPlan(Request, Session)}.
 *
 * <p>All nodes must be provided at construction time. After that, the only valid mutation operation
 * is {@link #poll()}, other methods throw.
 *
 * <p>This class is not a general-purpose implementation, it is tailored for a specific use case in
 * the driver. It makes a few unconventional API choices for the sake of performance (see {@link
 * #QueryPlan(Object...)}. It can be reused for custom load balancing policies; if you plan to do
 * so, study the source code of {@link DefaultLoadBalancingPolicy}.
 */
@ThreadSafe
public class QueryPlan extends AbstractCollection<Node> implements Queue<Node> {

  private final Object[] nodes;
  private final AtomicInteger nextIndex = new AtomicInteger();

  /**
   * @param nodes the nodes to initially fill the queue with. For efficiency, there is no defensive
   *     copy, the provided array is used directly. The declared type is {@code Object[]} because of
   *     implementation details of {@link DefaultLoadBalancingPolicy}, but all elements must be
   *     instances of {@link Node}, otherwise instance methods will fail later.
   */
  public QueryPlan(@NonNull Object... nodes) {
    this.nodes = nodes;
  }

  @Nullable
  @Override
  public Node poll() {
    // We don't handle overflow. In practice it won't be an issue, since the driver stops polling
    // once the query plan is empty.
    int i = nextIndex.getAndIncrement();
    return (i >= nodes.length) ? null : (Node) nodes[i];
  }

  /**
   * {@inheritDoc}
   *
   * <p>The returned iterator reflects the state of the queue at the time of the call, and is not
   * affected by further modifications.
   */
  @NonNull
  @Override
  public Iterator<Node> iterator() {
    int i = nextIndex.get();
    if (i >= nodes.length) {
      return Collections.<Node>emptyList().iterator();
    } else {
      return Iterators.forArray(Arrays.copyOfRange(nodes, i, nodes.length, Node[].class));
    }
  }

  @Override
  public int size() {
    return Math.max(nodes.length - nextIndex.get(), 0);
  }

  @Override
  public boolean offer(Node node) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Node remove() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Node element() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Node peek() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
