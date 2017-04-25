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
package com.datastax.oss.driver.internal.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.type.CustomType;
import com.datastax.oss.driver.api.type.DataType;
import com.datastax.oss.driver.api.type.codec.TypeCodec;
import com.datastax.oss.driver.api.type.reflect.GenericType;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;

public class CustomCodec implements TypeCodec<ByteBuffer> {

  private final CustomType cqlType;

  public CustomCodec(CustomType cqlType) {
    this.cqlType = cqlType;
  }

  @Override
  public GenericType<ByteBuffer> getJavaType() {
    return GenericType.BYTE_BUFFER;
  }

  @Override
  public DataType getCqlType() {
    return cqlType;
  }

  @Override
  public boolean canEncode(Object value) {
    return value instanceof ByteBuffer;
  }

  @Override
  public boolean canEncode(Class<?> javaClass) {
    return ByteBuffer.class.isAssignableFrom(javaClass);
  }

  @Override
  public ByteBuffer encode(ByteBuffer value, ProtocolVersion protocolVersion) {
    return (value == null) ? null : value.duplicate();
  }

  @Override
  public ByteBuffer decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    return (bytes == null) ? null : bytes.duplicate();
  }

  @Override
  public String format(ByteBuffer value) {
    return (value == null) ? "NULL" : Bytes.toHexString(value);
  }

  @Override
  public ByteBuffer parse(String value) {
    return (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
        ? null
        : Bytes.fromHexString(value);
  }
}
