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
package com.datastax.oss.driver.internal.core.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class DecimalCodec implements TypeCodec<BigDecimal> {
  @Override
  public GenericType<BigDecimal> getJavaType() {
    return GenericType.BIG_DECIMAL;
  }

  @Override
  public DataType getCqlType() {
    return DataTypes.DECIMAL;
  }

  @Override
  public boolean accepts(Object value) {
    return value instanceof BigDecimal;
  }

  @Override
  public boolean accepts(Class<?> javaClass) {
    return BigDecimal.class.isAssignableFrom(javaClass);
  }

  @Override
  public ByteBuffer encode(BigDecimal value, ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    }
    BigInteger bi = value.unscaledValue();
    int scale = value.scale();
    byte[] bibytes = bi.toByteArray();

    ByteBuffer bytes = ByteBuffer.allocate(4 + bibytes.length);
    bytes.putInt(scale);
    bytes.put(bibytes);
    bytes.rewind();
    return bytes;
  }

  @Override
  public BigDecimal decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return null;
    } else if (bytes.remaining() < 4) {
      throw new IllegalArgumentException(
          "Invalid decimal value, expecting at least 4 bytes but got " + bytes.remaining());
    }

    bytes = bytes.duplicate();
    int scale = bytes.getInt();
    byte[] bibytes = new byte[bytes.remaining()];
    bytes.get(bibytes);

    BigInteger bi = new BigInteger(bibytes);
    return new BigDecimal(bi, scale);
  }

  @Override
  public String format(BigDecimal value) {
    return (value == null) ? "NULL" : value.toString();
  }

  @Override
  public BigDecimal parse(String value) {
    try {
      return (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
          ? null
          : new BigDecimal(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Cannot parse decimal value from \"%s\"", value));
    }
  }
}
