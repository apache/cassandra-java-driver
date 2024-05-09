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
import com.datastax.oss.driver.internal.core.util.Strings;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class InetCodec implements TypeCodec<InetAddress> {
  @NonNull
  @Override
  public GenericType<InetAddress> getJavaType() {
    return GenericType.INET_ADDRESS;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.INET;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof InetAddress;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return InetAddress.class.equals(javaClass);
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable InetAddress value, @NonNull ProtocolVersion protocolVersion) {
    return (value == null) ? null : ByteBuffer.wrap(value.getAddress());
  }

  @Nullable
  @Override
  public InetAddress decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return null;
    }
    try {
      return InetAddress.getByAddress(Bytes.getArray(bytes));
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(
          "Invalid bytes for inet value, got " + bytes.remaining() + " bytes");
    }
  }

  @NonNull
  @Override
  public String format(@Nullable InetAddress value) {
    return (value == null) ? "NULL" : ("'" + value.getHostAddress() + "'");
  }

  @Nullable
  @Override
  public InetAddress parse(@Nullable String value) {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
      return null;
    }

    value = value.trim();
    if (!Strings.isQuoted(value)) {
      throw new IllegalArgumentException(
          String.format("inet values must be enclosed in single quotes (\"%s\")", value));
    }
    try {
      return InetAddress.getByName(value.substring(1, value.length() - 1));
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Cannot parse inet value from \"%s\"", value));
    }
  }
}
