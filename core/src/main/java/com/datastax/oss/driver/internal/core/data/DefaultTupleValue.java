/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;
import net.jcip.annotations.NotThreadSafe;

/**
 * Implementation note: contrary to most GettableBy* and SettableBy* implementations, this class is
 * mutable.
 */
@NotThreadSafe
public class DefaultTupleValue implements TupleValue, Serializable {

  private static final long serialVersionUID = 1;
  private final TupleType type;
  private final ByteBuffer[] values;

  public DefaultTupleValue(@NonNull TupleType type) {
    this(type, new ByteBuffer[type.getComponentTypes().size()]);
  }

  public DefaultTupleValue(@NonNull TupleType type, @NonNull Object... values) {
    this(
        type,
        ValuesHelper.encodeValues(
            values,
            type.getComponentTypes(),
            type.getAttachmentPoint().getCodecRegistry(),
            type.getAttachmentPoint().getProtocolVersion()));
  }

  private DefaultTupleValue(TupleType type, ByteBuffer[] values) {
    Preconditions.checkNotNull(type);
    this.type = type;
    this.values = values;
  }

  @NonNull
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

  @NonNull
  @Override
  public TupleValue setBytesUnsafe(int i, @Nullable ByteBuffer v) {
    values[i] = v;
    return this;
  }

  @NonNull
  @Override
  public DataType getType(int i) {
    return type.getComponentTypes().get(i);
  }

  @NonNull
  @Override
  public CodecRegistry codecRegistry() {
    return type.getAttachmentPoint().getCodecRegistry();
  }

  @NonNull
  @Override
  public ProtocolVersion protocolVersion() {
    return type.getAttachmentPoint().getProtocolVersion();
  }

  /**
   * @serialData The type of the tuple, followed by an array of byte arrays representing the values
   *     (null values are represented by {@code null}).
   */
  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  private void readObject(@SuppressWarnings("unused") ObjectInputStream stream)
      throws InvalidObjectException {
    // Should never be called since we serialized a proxy
    throw new InvalidObjectException("Proxy required");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof TupleValue)) {
      return false;
    }
    TupleValue that = (TupleValue) o;

    if (!type.equals(that.getType())) {
      return false;
    }

    for (int i = 0; i < values.length; i++) {
      DataType innerThisType = type.getComponentTypes().get(i);
      DataType innerThatType = that.getType().getComponentTypes().get(i);
      if (!innerThisType.equals(innerThatType)) {
        return false;
      }
      Object thisValue =
          this.codecRegistry()
              .codecFor(innerThisType)
              .decode(this.getBytesUnsafe(i), this.protocolVersion());
      Object thatValue =
          that.codecRegistry()
              .codecFor(innerThatType)
              .decode(that.getBytesUnsafe(i), that.protocolVersion());
      if (!Objects.equals(thisValue, thatValue)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {

    int result = type.hashCode();

    for (int i = 0; i < values.length; i++) {
      DataType innerThisType = type.getComponentTypes().get(i);
      Object thisValue =
          this.codecRegistry()
              .codecFor(innerThisType)
              .decode(this.values[i], this.protocolVersion());
      if (thisValue != null) {
        result = 31 * result + thisValue.hashCode();
      }
    }

    return result;
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
