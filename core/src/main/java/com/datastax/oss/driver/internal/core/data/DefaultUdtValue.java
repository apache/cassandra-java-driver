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
package com.datastax.oss.driver.internal.core.data;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.NotThreadSafe;

/**
 * Implementation note: contrary to most GettableBy* and SettableBy* implementations, this class is
 * mutable.
 */
@NotThreadSafe
public class DefaultUdtValue implements UdtValue, Serializable {

  private static final long serialVersionUID = 1;

  private final UserDefinedType type;
  private final ByteBuffer[] values;

  public DefaultUdtValue(@NonNull UserDefinedType type) {
    this(type, new ByteBuffer[type.getFieldTypes().size()]);
  }

  public DefaultUdtValue(@NonNull UserDefinedType type, @NonNull Object... values) {
    this(
        type,
        ValuesHelper.encodeValues(
            values,
            type.getFieldTypes(),
            type.getAttachmentPoint().getCodecRegistry(),
            type.getAttachmentPoint().getProtocolVersion()));
  }

  private DefaultUdtValue(UserDefinedType type, ByteBuffer[] values) {
    Preconditions.checkNotNull(type);
    this.type = type;
    this.values = values;
  }

  @NonNull
  @Override
  public UserDefinedType getType() {
    return type;
  }

  @Override
  public int size() {
    return values.length;
  }

  @NonNull
  @Override
  public List<Integer> allIndicesOf(@NonNull CqlIdentifier id) {
    List<Integer> indices = type.allIndicesOf(id);
    if (indices.isEmpty()) {
      throw new IllegalArgumentException(id + " is not a field in this UDT");
    }
    return indices;
  }

  @Override
  public int firstIndexOf(@NonNull CqlIdentifier id) {
    int indexOf = type.firstIndexOf(id);
    if (indexOf == -1) {
      throw new IllegalArgumentException(id + " is not a field in this UDT");
    }
    return indexOf;
  }

  @NonNull
  @Override
  public List<Integer> allIndicesOf(@NonNull String name) {
    List<Integer> indices = type.allIndicesOf(name);
    if (indices.isEmpty()) {
      throw new IllegalArgumentException(name + " is not a field in this UDT");
    }
    return indices;
  }

  @Override
  public int firstIndexOf(@NonNull String name) {
    int indexOf = type.firstIndexOf(name);
    if (indexOf == -1) {
      throw new IllegalArgumentException(name + " is not a field in this UDT");
    }
    return indexOf;
  }

  @NonNull
  @Override
  public DataType getType(int i) {
    return type.getFieldTypes().get(i);
  }

  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return values[i];
  }

  @NonNull
  @Override
  public UdtValue setBytesUnsafe(int i, @Nullable ByteBuffer v) {
    values[i] = v;
    return this;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof UdtValue)) {
      return false;
    }
    UdtValue that = (UdtValue) o;

    if (!type.equals(that.getType())) {
      return false;
    }

    for (int i = 0; i < values.length; i++) {

      DataType innerThisType = type.getFieldTypes().get(i);
      DataType innerThatType = that.getType().getFieldTypes().get(i);

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
      DataType innerThisType = type.getFieldTypes().get(i);
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

  private static class SerializationProxy implements Serializable {

    private static final long serialVersionUID = 1;

    private final UserDefinedType type;
    private final byte[][] values;

    SerializationProxy(DefaultUdtValue udt) {
      this.type = udt.type;
      this.values = new byte[udt.values.length][];
      for (int i = 0; i < udt.values.length; i++) {
        ByteBuffer buffer = udt.values[i];
        this.values[i] = (buffer == null) ? null : Bytes.getArray(buffer);
      }
    }

    private Object readResolve() {
      ByteBuffer[] buffers = new ByteBuffer[this.values.length];
      for (int i = 0; i < this.values.length; i++) {
        byte[] value = this.values[i];
        buffers[i] = (value == null) ? null : ByteBuffer.wrap(value);
      }
      return new DefaultUdtValue(this.type, buffers);
    }
  }
}
