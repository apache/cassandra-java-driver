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

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.CustomPayload;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultCustomPayload implements CustomPayload {

  private final ImmutableMap<String, DataType> columns;
  private final ImmutableMap<String, ByteBuffer> values;
  private transient volatile AttachmentPoint attachmentPoint;

  public DefaultCustomPayload(Map<String, DataType> columns, AttachmentPoint attachmentPoint) {
    this(columns, new HashMap<>(), attachmentPoint);
  }

  private DefaultCustomPayload(
      Map<String, DataType> columns,
      Map<String, ByteBuffer> values,
      AttachmentPoint attachmentPoint) {
    this.columns = ImmutableMap.copyOf(columns);
    this.values = ImmutableMap.copyOf(values);
    this.attachmentPoint = attachmentPoint;
  }

  @Override
  public Map<String, ByteBuffer> getValues() {
    return values;
  }

  @Override
  public ByteBuffer getBytesUnsafe(String name) {
    return values.get(name);
  }

  @Override
  public CustomPayload setBytesUnsafe(String name, ByteBuffer v) {
    return new DefaultCustomPayload(
        columns,
        ImmutableMap.<String, ByteBuffer>builder().putAll(values).put(name, v).build(),
        attachmentPoint);
  }

  @Override
  public DataType getType(String name) {
    return columns.get(name);
  }

  @Override
  public int size() {
    return columns.size();
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
    return attachmentPoint == AttachmentPoint.NONE;
  }

  @Override
  public void attach(AttachmentPoint attachmentPoint) {
    this.attachmentPoint = attachmentPoint;
  }

  /**
   * @serialData The column definitions, followed by an array of byte arrays representing the column
   *     values (null values are represented by {@code null}).
   */
  private Object writeReplace() {
    return new DefaultCustomPayload.SerializationProxy(this);
  }

  private void readObject(ObjectInputStream stream) throws InvalidObjectException {
    // Should never be called since we serialized a proxy
    throw new InvalidObjectException("Proxy required");
  }

  private static class SerializationProxy implements Serializable {

    private static final long serialVersionUID = 1;

    private final Map<String, DataType> columns;
    private final Map<String, ByteBuffer> values;

    SerializationProxy(DefaultCustomPayload payload) {
      this.columns = payload.columns;
      this.values = payload.values;
    }

    private Object readResolve() {
      return new DefaultCustomPayload(columns, values, AttachmentPoint.NONE);
    }
  }
}
