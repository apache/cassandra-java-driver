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
package com.datastax.oss.driver.internal.core.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.jcip.annotations.ThreadSafe;

/**
 * A codec that maps the CQL type {@code blob} to the Java type {@link ByteBuffer}.
 *
 * <p>If you are looking for a codec mapping the CQL type {@code blob} to the Java type {@code
 * byte[]}, you should use {@link SimpleBlobCodec} instead.
 */
@ThreadSafe
public class BlobCodec implements TypeCodec<ByteBuffer> {
  @Nonnull
  @Override
  public GenericType<ByteBuffer> getJavaType() {
    return GenericType.BYTE_BUFFER;
  }

  @Nonnull
  @Override
  public DataType getCqlType() {
    return DataTypes.BLOB;
  }

  @Override
  public boolean accepts(@Nonnull Object value) {
    return value instanceof ByteBuffer;
  }

  @Override
  public boolean accepts(@Nonnull Class<?> javaClass) {
    return ByteBuffer.class.equals(javaClass);
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable ByteBuffer value, @Nonnull ProtocolVersion protocolVersion) {
    return (value == null) ? null : value.duplicate();
  }

  @Nullable
  @Override
  public ByteBuffer decode(@Nullable ByteBuffer bytes, @Nonnull ProtocolVersion protocolVersion) {
    return (bytes == null) ? null : bytes.duplicate();
  }

  @Nonnull
  @Override
  public String format(@Nullable ByteBuffer value) {
    return (value == null) ? "NULL" : Bytes.toHexString(value);
  }

  @Nullable
  @Override
  public ByteBuffer parse(@Nullable String value) {
    return (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
        ? null
        : Bytes.fromHexString(value);
  }
}
