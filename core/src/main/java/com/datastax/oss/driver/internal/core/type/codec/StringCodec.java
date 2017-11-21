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
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.util.Strings;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class StringCodec implements TypeCodec<String> {

  private final DataType cqlType;
  private final Charset charset;

  public StringCodec(DataType cqlType, Charset charset) {
    this.cqlType = cqlType;
    this.charset = charset;
  }

  @Override
  public GenericType<String> getJavaType() {
    return GenericType.STRING;
  }

  @Override
  public DataType getCqlType() {
    return cqlType;
  }

  @Override
  public boolean accepts(Object value) {
    return value instanceof String;
  }

  @Override
  public boolean accepts(Class<?> javaClass) {
    return javaClass == String.class;
  }

  @Override
  public ByteBuffer encode(String value, ProtocolVersion protocolVersion) {
    return (value == null) ? null : ByteBuffer.wrap(value.getBytes(charset));
  }

  @Override
  public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    if (bytes == null) {
      return null;
    } else if (bytes.remaining() == 0) {
      return "";
    } else {
      return new String(Bytes.getArray(bytes), charset);
    }
  }

  @Override
  public String format(String value) {
    return (value == null) ? "NULL" : Strings.quote(value);
  }

  @Override
  public String parse(String value) {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
      return null;
    } else if (!Strings.isQuoted(value)) {
      throw new IllegalArgumentException(
          "text or varchar values must be enclosed by single quotes");
    } else {
      return Strings.unquote(value);
    }
  }
}
