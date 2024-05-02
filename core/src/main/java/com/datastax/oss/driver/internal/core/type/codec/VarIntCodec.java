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
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class VarIntCodec implements TypeCodec<BigInteger> {
  @NonNull
  @Override
  public GenericType<BigInteger> getJavaType() {
    return GenericType.BIG_INTEGER;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.VARINT;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof BigInteger;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return BigInteger.class.isAssignableFrom(javaClass);
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable BigInteger value, @NonNull ProtocolVersion protocolVersion) {
    return (value == null) ? null : ByteBuffer.wrap(value.toByteArray());
  }

  @Nullable
  @Override
  public BigInteger decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    return (bytes == null) || bytes.remaining() == 0 ? null : new BigInteger(Bytes.getArray(bytes));
  }

  @NonNull
  @Override
  public String format(@Nullable BigInteger value) {
    return (value == null) ? "NULL" : value.toString();
  }

  @Nullable
  @Override
  public BigInteger parse(@Nullable String value) {
    try {
      return (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
          ? null
          : new BigInteger(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Cannot parse varint value from \"%s\"", value), e);
    }
  }
}
