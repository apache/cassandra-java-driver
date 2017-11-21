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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DefaultRow implements Row {

  private final ColumnDefinitions definitions;
  private final List<ByteBuffer> data;
  private transient volatile AttachmentPoint attachmentPoint;

  public DefaultRow(
      ColumnDefinitions definitions, List<ByteBuffer> data, AttachmentPoint attachmentPoint) {
    this.definitions = definitions;
    this.data = data;
    this.attachmentPoint = attachmentPoint;
  }

  public DefaultRow(ColumnDefinitions definitions, List<ByteBuffer> data) {
    this(definitions, data, AttachmentPoint.NONE);
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return definitions;
  }

  @Override
  public int size() {
    return definitions.size();
  }

  @Override
  public DataType getType(int i) {
    return definitions.get(i).getType();
  }

  @Override
  public int firstIndexOf(CqlIdentifier id) {
    return definitions.firstIndexOf(id);
  }

  @Override
  public DataType getType(CqlIdentifier id) {
    return definitions.get(firstIndexOf(id)).getType();
  }

  @Override
  public int firstIndexOf(String name) {
    return definitions.firstIndexOf(name);
  }

  @Override
  public DataType getType(String name) {
    return definitions.get(firstIndexOf(name)).getType();
  }

  @Override
  public CodecRegistry codecRegistry() {
    return attachmentPoint.codecRegistry();
  }

  @Override
  public ProtocolVersion protocolVersion() {
    return attachmentPoint.protocolVersion();
  }

  @Override
  public boolean isDetached() {
    return attachmentPoint != AttachmentPoint.NONE;
  }

  @Override
  public void attach(AttachmentPoint attachmentPoint) {
    this.attachmentPoint = attachmentPoint;
    this.definitions.attach(attachmentPoint);
  }

  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return data.get(i);
  }
  /**
   * @serialData The column definitions, followed by an array of byte arrays representing the column
   *     values (null values are represented by {@code null}).
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

    private final ColumnDefinitions definitions;
    private final byte[][] values;

    SerializationProxy(DefaultRow row) {
      this.definitions = row.definitions;
      this.values = new byte[row.data.size()][];
      int i = 0;
      for (ByteBuffer buffer : row.data) {
        this.values[i] = (buffer == null) ? null : Bytes.getArray(buffer);
        i += 1;
      }
    }

    private Object readResolve() {
      List<ByteBuffer> data = new ArrayList<>(this.values.length);
      for (byte[] value : this.values) {
        data.add((value == null) ? null : ByteBuffer.wrap(value));
      }
      return new DefaultRow(this.definitions, data);
    }
  }
}
