/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;

public class GraphResultSets {

  public static GraphResultSet toSync(AsyncGraphResultSet firstPage) {
    if (firstPage.hasMorePages()) {
      throw new UnsupportedOperationException("TODO implement multi-page results");
    } else {
      return new SinglePageGraphResultSet(firstPage);
    }
  }
}
