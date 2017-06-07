/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.data;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.base.Preconditions;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class DefaultTupleValue implements TupleValue {

  private static final long serialVersionUID = 1;

  private final TupleType type;
  private final ByteBuffer[] values;

  public DefaultTupleValue(TupleType type) {
    this(type, new ByteBuffer[type.getComponentTypes().size()]);
  }

  private DefaultTupleValue(TupleType type, ByteBuffer[] values) {
    Preconditions.checkNotNull(type);
    this.type = type;
    this.values = values;
  }

  @Override
  public TupleType getType() {
    return type;
  }

  @Override
  public int size() {
    return values.length;
  }

  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return values[i];
  }

  @Override
  public TupleValue setBytesUnsafe(int i, ByteBuffer v) {
    values[i] = v;
    return this;
  }

  @Override
  public DataType getType(int i) {
    return type.getComponentTypes().get(i);
  }

  @Override
  public CodecRegistry codecRegistry() {
    return type.getAttachmentPoint().codecRegistry();
  }

  @Override
  public ProtocolVersion protocolVersion() {
    return type.getAttachmentPoint().protocolVersion();
  }

  /**
   * @serialData The type of the tuple, followed by an array of byte arrays representing the values
   *     (null values are represented by {@code null}).
   */
  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  private void readObject(ObjectInputStream stream) throws InvalidObjectException {
    // Should never be called since we serialized a proxy
    throw new InvalidObjectException("Proxy required");
  }

  private static class SerializationProxy implements Serializable {

    private static final long serialVersionUID = 1;

    private final TupleType type;
    private final byte[][] values;

    SerializationProxy(DefaultTupleValue tuple) {
      this.type = tuple.type;
      this.values = new byte[tuple.values.length][];
      for (int i = 0; i < tuple.values.length; i++) {
        ByteBuffer buffer = tuple.values[i];
        this.values[i] = (buffer == null) ? null : Bytes.getArray(buffer);
      }
    }

    private Object readResolve() {
      ByteBuffer[] buffers = new ByteBuffer[this.values.length];
      for (int i = 0; i < this.values.length; i++) {
        byte[] value = this.values[i];
        buffers[i] = (value == null) ? null : ByteBuffer.wrap(value);
      }
      return new DefaultTupleValue(this.type, buffers);
    }
  }
}
