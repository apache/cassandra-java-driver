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
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.util.Strings;
import com.datastax.oss.driver.shaded.guava.common.base.Optional;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.util.concurrent.FastThreadLocal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class StringCodec implements TypeCodec<String> {

  private final DataType cqlType;
  private final FastThreadLocal<CharsetEncoder> charsetEncoder;
  private final FastThreadLocal<CharsetDecoder> charsetDecoder;

  public StringCodec(@NonNull DataType cqlType, @NonNull Charset charset) {
    this.cqlType = cqlType;
    charsetEncoder =
        new FastThreadLocal<CharsetEncoder>() {
          @Override
          protected CharsetEncoder initialValue() throws Exception {
            return charset
                .newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
          }
        };
    charsetDecoder =
        new FastThreadLocal<CharsetDecoder>() {
          @Override
          protected CharsetDecoder initialValue() throws Exception {
            return charset
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
          }
        };
  }

  @NonNull
  @Override
  public GenericType<String> getJavaType() {
    return GenericType.STRING;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return cqlType;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof String;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return javaClass == String.class;
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable String value, @NonNull ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    }
    try {
      return charsetEncoder.get().encode(CharBuffer.wrap(value));
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Nullable
  @Override
  public String decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null) {
      return null;
    } else if (bytes.remaining() == 0) {
      return "";
    } else {
      try {
        return charsetDecoder.get().decode(bytes.duplicate()).toString();
      } catch (CharacterCodingException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  @NonNull
  @Override
  public String format(@Nullable String value) {
    return (value == null) ? "NULL" : Strings.quote(value);
  }

  @Nullable
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

  @NonNull
  @Override
  public Optional<Integer> serializedSize() {
    return Optional.absent();
  }
}
