/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.dse.driver.api.core.data.geometry.Geometry;
import com.datastax.dse.driver.internal.core.graph.TinkerpopBufferUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;

public abstract class GeometrySerializer<T extends Geometry>
    extends AbstractSimpleGraphBinaryCustomSerializer<T> {
  public abstract T fromWellKnownBinary(ByteBuffer buffer);

  @Override
  protected T readCustomValue(int valueLength, Buffer buffer, GraphBinaryReader context)
      throws IOException {
    return fromWellKnownBinary(TinkerpopBufferUtil.readBytes(buffer, valueLength));
  }

  @Override
  protected void writeCustomValue(T value, Buffer buffer, GraphBinaryWriter context)
      throws IOException {
    ByteBuffer bb = value.asWellKnownBinary();

    // writing the {value_length}
    context.writeValue(bb.remaining(), buffer, false);
    buffer.writeBytes(bb);
  }
}
