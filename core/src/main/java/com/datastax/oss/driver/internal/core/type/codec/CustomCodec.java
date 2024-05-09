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
import com.datastax.oss.driver.api.core.type.CustomType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class CustomCodec implements TypeCodec<ByteBuffer> {

  private final CustomType cqlType;

  public CustomCodec(CustomType cqlType) {
    this.cqlType = cqlType;
  }

  @NonNull
  @Override
  public GenericType<ByteBuffer> getJavaType() {
    return GenericType.BYTE_BUFFER;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return cqlType;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof ByteBuffer;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return ByteBuffer.class.equals(javaClass);
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable ByteBuffer value, @NonNull ProtocolVersion protocolVersion) {
    return (value == null) ? null : value.duplicate();
  }

  @Nullable
  @Override
  public ByteBuffer decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    return (bytes == null) ? null : bytes.duplicate();
  }

  @NonNull
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
