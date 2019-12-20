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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.util.Queue;
import net.jcip.annotations.NotThreadSafe;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

@NotThreadSafe // wraps a mutable queue
class GraphResultIterator extends CountingIterator<GraphNode> {

  private final Queue<GraphNode> data;
  private final GraphProtocol graphProtocol;

  // Sometimes a traversal can yield the same result multiple times consecutively. To avoid
  // duplicating the data, DSE graph sends it only once with a counter indicating how many times
  // it's repeated.
  private long repeat = 0;
  private GraphNode lastGraphNode = null;

  GraphResultIterator(Queue<GraphNode> data, GraphProtocol graphProtocol) {
    super(data.size());
    this.data = data;
    this.graphProtocol = graphProtocol;
  }

  @Override
  protected GraphNode computeNext() {
    if (repeat > 1) {
      repeat -= 1;
      // Note that we don't make a defensive copy, we assume the client won't mutate the node
      return lastGraphNode;
    }

    GraphNode container = data.poll();
    if (container == null) {
      return endOfData();
    }

    if (graphProtocol.isGraphBinary()) {
      // results are contained in a Traverser object and not a Map if the protocol
      // is GraphBinary
      Preconditions.checkState(
          container.as(Object.class) instanceof Traverser,
          "Graph protocol error. Received object should be a Traverser but it is not.");
      Traverser t = container.as(Traverser.class);
      this.repeat = t.bulk();
      this.lastGraphNode = new ObjectGraphNode(t.get());
      return lastGraphNode;
    } else {
      // The repeat counter is called "bulk" in the JSON payload
      GraphNode b = container.getByKey("bulk");
      if (b != null) {
        this.repeat = b.asLong();
      }

      lastGraphNode = container.getByKey("result");
      return lastGraphNode;
    }
  }
}
