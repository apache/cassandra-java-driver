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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.ThreadSafe;

/** A query plan that encompasses many child plans, and consumes them one by one. */
@ThreadSafe
public class CompositeQueryPlan extends AbstractQueue<Node> implements QueryPlan {

  private final Queue<Node>[] plans;
  private final AtomicInteger currentPlan = new AtomicInteger(0);

  @SafeVarargs
  public CompositeQueryPlan(@NonNull Queue<Node>... plans) {
    if (plans.length == 0) {
      throw new IllegalArgumentException("at least one child plan must be provided");
    }
    for (Queue<Node> plan : plans) {
      if (plan == null) {
        throw new NullPointerException("child plan cannot be null");
      }
    }
    this.plans = plans;
  }

  @Nullable
  @Override
  public Node poll() {
    while (true) {
      int current = currentPlan.get();
      Queue<Node> plan = plans[current];
      Node n = plan.poll();
      if (n != null) {
        return n;
      }
      int next = current + 1;
      if (next == plans.length) {
        return null;
      }
      currentPlan.compareAndSet(current, next);
    }
  }

  @NonNull
  @Override
  public Iterator<Node> iterator() {
    List<Iterator<Node>> its = new ArrayList<>(plans.length);
    for (Queue<Node> plan : plans) {
      its.add(plan.iterator());
    }
    return Iterators.concat(its.iterator());
  }

  @Override
  public int size() {
    int size = 0;
    for (Queue<Node> plan : plans) {
      size += plan.size();
    }
    return size;
  }
}
