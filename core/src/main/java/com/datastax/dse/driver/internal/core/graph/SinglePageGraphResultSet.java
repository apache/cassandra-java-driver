/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphExecutionInfo;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Iterator;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class SinglePageGraphResultSet implements GraphResultSet {

  private final AsyncGraphResultSet onlyPage;

  public SinglePageGraphResultSet(AsyncGraphResultSet onlyPage) {
    this.onlyPage = onlyPage;
    assert !onlyPage.hasMorePages();
  }

  @NonNull
  @Override
  public GraphExecutionInfo getExecutionInfo() {
    return onlyPage.getExecutionInfo();
  }

  @NonNull
  @Override
  public Iterator<GraphNode> iterator() {
    return onlyPage.currentPage().iterator();
  }

  @Override
  public void cancel() {
    // nothing to do
  }
}
