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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import java.util.Iterator;
import java.util.NoSuchElementException;
import net.jcip.annotations.NotThreadSafe;
import org.apache.tinkerpop.gremlin.process.remote.traversal.AbstractRemoteTraversal;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

@NotThreadSafe
class DseGraphTraversal<S, E> extends AbstractRemoteTraversal<S, E> {

  private final Iterator<GraphNode> graphNodeIterator;

  public DseGraphTraversal(AsyncGraphResultSet firstPage) {
    this.graphNodeIterator = GraphResultSets.toSync(firstPage).iterator();
  }

  @Override
  public boolean hasNext() {
    return graphNodeIterator.hasNext();
  }

  @Override
  public E next() {
    return nextTraverser().get();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Traverser.Admin<E> nextTraverser() {
    if (hasNext()) {
      GraphNode nextGraphNode = graphNodeIterator.next();

      // get the Raw object from the ObjectGraphNode, create a new remote Traverser
      // with bulk = 1 because bulk is not supported yet. Casting should be ok
      // because we have been able to deserialize into the right type.
      return new DefaultRemoteTraverser<>((E) nextGraphNode.as(Object.class), 1);
    } else {
      // finished iterating/nothing to iterate. Normal behaviour.
      throw new NoSuchElementException();
    }
  }
}
