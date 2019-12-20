/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
