/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import edu.umd.cs.findbugs.annotations.NonNull;

/** Vertex metadata, for a table that was created with CREATE TABLE ... WITH VERTEX LABEL. */
public interface DseVertexMetadata {

  /** The label of the vertex in graph. */
  @NonNull
  CqlIdentifier getLabelName();
}
