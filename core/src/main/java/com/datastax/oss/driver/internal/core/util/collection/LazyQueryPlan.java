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
import net.jcip.annotations.ThreadSafe;

/**
 * A query plan where nodes are computed lazily, when the plan is consumed for the first time.
 *
 * <p>This class can be useful when a query plan computation is heavy but the plan has a low chance
 * of ever being consumed, e.g. the last query plan in a {@link CompositeQueryPlan}.
 */
@ThreadSafe
public abstract class LazyQueryPlan extends QueryPlanBase {

  private volatile Object[] nodes;

  /**
   * Computes and returns the nodes to use for this query plan.
   *
   * <p>For efficiency, the declared return type is {@code Object[]} but all elements must be
   * instances of {@link Node}. See {@link #getNodes()} for details.
   *
   * <p>This method is guaranteed to be invoked only once, at the first call to {@link #poll()}.
   *
   * <p>Implementors must avoid blocking calls in this method as it will be invoked on the driver's
   * hot path.
   */
  protected abstract Object[] computeNodes();

  @Override
  protected Object[] getNodes() {
    if (nodes == null) {
      synchronized (this) {
        if (nodes == null) {
          nodes = computeNodes();
        }
      }
    }
    return nodes;
  }
}
