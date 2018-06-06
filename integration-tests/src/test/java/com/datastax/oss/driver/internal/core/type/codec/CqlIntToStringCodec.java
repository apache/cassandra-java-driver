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
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.nio.ByteBuffer;

/**
 * A sample user codec implementation that we use in our tests.
 *
 * <p>It maps a CQL string to a Java string containing its textual representation.
 */
public class CqlIntToStringCodec implements TypeCodec<String> {

  @Override
  public GenericType<String> getJavaType() {
    return GenericType.STRING;
  }

  @Override
  public DataType getCqlType() {
    return DataTypes.INT;
  }

  @Override
  public ByteBuffer encode(String value, ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    } else {
      return TypeCodecs.INT.encode(Integer.parseInt(value), protocolVersion);
    }
  }

  @Override
  public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    return TypeCodecs.INT.decode(bytes, protocolVersion).toString();
  }

  @Override
  public String format(String value) {
    throw new UnsupportedOperationException("Not implemented for this test");
  }

  @Override
  public String parse(String value) {
    throw new UnsupportedOperationException("Not implemented for this test");
  }
}
