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
package com.datastax.oss.driver.internal.core.util.collection;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterators;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public abstract class QueryPlanBase extends AbstractQueue<Node> implements QueryPlan {

  private final AtomicInteger nextIndex = new AtomicInteger();

  /**
   * Returns the nodes in this query plan; the returned array should stay the same across
   * invocations.
   *
   * <p>The declared return type is {@code Object[]} because of implementation details of {@link
   * com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy
   * DefaultLoadBalancingPolicy} and {@link
   * com.datastax.oss.driver.internal.core.loadbalancing.BasicLoadBalancingPolicy
   * BasicLoadBalancingPolicy}, but all elements must be instances of {@link Node}, otherwise
   * instance methods will fail later.
   */
  protected abstract Object[] getNodes();

  @Nullable
  @Override
  public Node poll() {
    // We don't handle overflow. In practice it won't be an issue, since the driver stops polling
    // once the query plan is empty.
    int i = nextIndex.getAndIncrement();
    Object[] nodes = getNodes();
    return (i >= nodes.length) ? null : (Node) nodes[i];
  }

  @NonNull
  @Override
  public Iterator<Node> iterator() {
    int i = nextIndex.get();
    Object[] nodes = getNodes();
    if (i >= nodes.length) {
      return Collections.emptyIterator();
    } else {
      return Iterators.forArray(Arrays.copyOfRange(nodes, i, nodes.length, Node[].class));
    }
  }

  @Override
  public int size() {
    return Math.max(getNodes().length - nextIndex.get(), 0);
  }
}
