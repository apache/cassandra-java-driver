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
import com.datastax.oss.driver.shaded.guava.common.collect.Iterators;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.AbstractQueue;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.ThreadSafe;

/**
 * Query plan over a fixed array, where the elements from a start index to the end are rotated to
 * the left (round-robin): with a rotated range of length {@code n}, the element at {@code
 * startIndex + amount % n} comes first in that range.
 *
 * <p>The elements are not moved. Instead, positions are mapped as nodes are polled. This makes
 * building the plan independent of the size of the rotated range, at a small constant cost per node
 * polled.
 */
@ThreadSafe
public class RotatedQueryPlan extends AbstractQueue<Node> implements QueryPlan {

  private final Object[] nodes;
  private final int startIndex;
  private final int amount;
  private final AtomicInteger nextIndex = new AtomicInteger();

  /**
   * Creates a new query plan with the given nodes.
   *
   * <p>For efficiency, there is no defensive copy, the provided array is used directly and must not
   * be modified afterwards. The declared type is {@code Object[]} but all elements must be
   * instances of {@link Node}.
   *
   * @param nodes the nodes.
   * @param startIndex the index of the first element of the rotated range, which extends to the end
   *     of the array. Must be between 0 and {@code nodes.length} inclusive.
   * @param amount the amount to rotate by. Zero or a negative amount means no rotation.
   */
  public RotatedQueryPlan(@NonNull Object[] nodes, int startIndex, int amount) {
    this.nodes = nodes;
    this.startIndex = startIndex;
    int length = nodes.length - startIndex;
    this.amount = (length >= 2 && amount > 0) ? amount % length : 0;
  }

  @Nullable
  @Override
  public Node poll() {
    // We don't handle overflow. In practice it won't be an issue, since the driver stops polling
    // once the query plan is empty.
    int i = nextIndex.getAndIncrement();
    return (i >= nodes.length) ? null : (Node) nodes[map(i)];
  }

  @NonNull
  @Override
  public Iterator<Node> iterator() {
    int i = nextIndex.get();
    if (i >= nodes.length) {
      return Collections.emptyIterator();
    }
    Node[] remaining = new Node[nodes.length - i];
    for (int j = 0; j < remaining.length; j++) {
      remaining[j] = (Node) nodes[map(i + j)];
    }
    return Iterators.forArray(remaining);
  }

  @Override
  public int size() {
    return Math.max(nodes.length - nextIndex.get(), 0);
  }

  private int map(int index) {
    if (index < startIndex) {
      return index;
    }
    int mapped = index + amount;
    return (mapped < nodes.length) ? mapped : mapped - (nodes.length - startIndex);
  }
}
