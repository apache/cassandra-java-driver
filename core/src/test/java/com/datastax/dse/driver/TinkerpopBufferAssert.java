/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.assertj.core.api.AbstractAssert;

public class TinkerpopBufferAssert extends AbstractAssert<TinkerpopBufferAssert, Buffer> {
  public TinkerpopBufferAssert(Buffer actual) {
    super(actual, TinkerpopBufferAssert.class);
  }

  public TinkerpopBufferAssert containsExactly(String hexString) {

    byte[] expectedBytes = Bytes.fromHexString(hexString).array();
    byte[] actualBytes = new byte[expectedBytes.length];
    actual.readBytes(actualBytes);
    assertThat(actualBytes).containsExactly(expectedBytes);
    assertThat(actual.readableBytes()).isEqualTo(0);
    return this;
  }
}
