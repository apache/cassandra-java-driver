/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver;

import org.apache.tinkerpop.gremlin.structure.io.Buffer;

public class Assertions extends org.assertj.core.api.Assertions {
  public static TinkerpopBufferAssert assertThat(Buffer actual) {
    return new TinkerpopBufferAssert(actual);
  }
}
