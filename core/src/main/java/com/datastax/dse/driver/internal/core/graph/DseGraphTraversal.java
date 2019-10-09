/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import java.util.Iterator;
import java.util.NoSuchElementException;
import net.jcip.annotations.NotThreadSafe;
import org.apache.tinkerpop.gremlin.process.remote.traversal.AbstractRemoteTraversal;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

@NotThreadSafe
class DseGraphTraversal<S, E> extends AbstractRemoteTraversal<S, E> {

  private final Iterator<GraphNode> graphNodeIterator;

  public DseGraphTraversal(AsyncGraphResultSet firstPage) {
    this.graphNodeIterator = GraphResultSets.toSync(firstPage).iterator();
  }

  @Override
  public RemoteTraversalSideEffects getSideEffects() {
    // return null but do not throw "NotSupportedException"
    return null;
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
