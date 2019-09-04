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
package com.datastax.oss.driver.api.querybuilder;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.datastax.oss.driver.internal.querybuilder.DefaultLiteral;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/** Example codec implementation used for {@link DefaultLiteral} tests. */
public class CharsetCodec implements TypeCodec<Charset> {

  /** A registry that contains an instance of this codec. */
  public static final CodecRegistry TEST_REGISTRY;

  static {
    MutableCodecRegistry registry = new DefaultCodecRegistry("test");
    registry.register(new CharsetCodec());
    TEST_REGISTRY = registry;
  }

  @NonNull
  @Override
  public GenericType<Charset> getJavaType() {
    return GenericType.of(Charset.class);
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.TEXT;
  }

  @NonNull
  @Override
  public String format(Charset value) {
    return "'" + value.name() + "'";
  }

  @Override
  public ByteBuffer encode(Charset value, @NonNull ProtocolVersion protocolVersion) {
    throw new UnsupportedOperationException("Not used in this test");
  }

  @Override
  public Charset decode(ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    throw new UnsupportedOperationException("Not used in this test");
  }

  @Override
  public Charset parse(String value) {
    throw new UnsupportedOperationException("Not used in this test");
  }
}
