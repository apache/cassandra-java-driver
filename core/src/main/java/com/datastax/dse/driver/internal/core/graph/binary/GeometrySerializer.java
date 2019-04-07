/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.dse.driver.api.core.data.geometry.Geometry;
import com.datastax.dse.driver.internal.core.graph.ByteBufUtil;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;

public abstract class GeometrySerializer<T extends Geometry>
    extends AbstractSimpleGraphBinaryCustomSerializer<T> {
  public abstract T fromWellKnownBinary(ByteBuffer buffer);

  @Override
  protected T readCustomValue(int valueLength, ByteBuf buffer, GraphBinaryReader context)
      throws SerializationException {
    return fromWellKnownBinary(ByteBufUtil.readBytes(buffer, valueLength));
  }

  @Override
  protected void writeCustomValue(T value, ByteBuf buffer, GraphBinaryWriter context)
      throws SerializationException {
    ByteBuffer bb = value.asWellKnownBinary();

    // writing the {value_length}
    context.writeValue(bb.remaining(), buffer, false);
    buffer.writeBytes(bb);
  }
}
