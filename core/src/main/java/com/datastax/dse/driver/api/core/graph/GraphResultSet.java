/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * The result of a synchronous Graph query.
 *
 * <p>This object is a container for {@link GraphNode} objects that will contain the data returned
 * by Graph queries.
 *
 * <p>Note that this object can only be iterated once: items are "consumed" as they are read,
 * subsequent calls to {@code iterator()} will return the same iterator instance.
 *
 * <p>The default implementation returned by the driver is <b>not</b> thread-safe. It can only be
 * iterated by the thread that invoked {@code dseSession.execute}.
 *
 * @see GraphNode
 * @see GraphSession#execute(GraphStatement)
 */
public interface GraphResultSet extends Iterable<GraphNode> {

  /**
   * Returns the next node, or {@code null} if the result set is exhausted.
   *
   * <p>This is convenient for queries that are known to return exactly one row, for example count
   * queries.
   */
  @Nullable
  default GraphNode one() {
    Iterator<GraphNode> graphNodeIterator = iterator();
    return graphNodeIterator.hasNext() ? graphNodeIterator.next() : null;
  }

  /**
   * Returns all the remaining nodes as a list; <b>not recommended for paginated queries that return
   * a large number of nodes</b>.
   *
   * <p>At this time (DSE 6.0.0), graph queries are not paginated and the server sends all the
   * results at once.
   */
  @NonNull
  default List<GraphNode> all() {
    if (!iterator().hasNext()) {
      return Collections.emptyList();
    }
    return ImmutableList.copyOf(this);
  }

  /**
   * Cancels the query and asks the server to stop sending results.
   *
   * <p>At this time (DSE 6.0.0), graph queries are not paginated and the server sends all the
   * results at once; therefore this method has no effect.
   */
  void cancel();

  /**
   * The execution information for the query that have been performed to assemble this result set.
   */
  @NonNull
  GraphExecutionInfo getExecutionInfo();
}
