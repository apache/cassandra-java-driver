/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import java.nio.ByteBuffer;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;

/** Mirror of {@link ByteBufUtil} for Tinkerpop Buffer's */
public class TinkerpopBufferUtil {

  public static ByteBuffer readBytes(Buffer tinkerBuff, int size) {
    ByteBuffer res = ByteBuffer.allocate(size);
    tinkerBuff.readBytes(res);
    res.flip();
    return res;
  }
}
