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
package com.datastax.oss.driver.internal.core.type.codec.extras.array;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.CodecTestBase;
import com.datastax.oss.protocol.internal.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ObjectArrayCodecTest extends CodecTestBase<String[]> {

  @Mock private TypeCodec<String> elementCodec;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(elementCodec.getCqlType()).thenReturn(DataTypes.TEXT);
    when(elementCodec.getJavaType()).thenReturn(GenericType.STRING);
    codec = ExtraTypeCodecs.listToArrayOf(elementCodec);
  }

  @Test
  public void should_encode_null() {
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_encode_empty_array() {
    assertThat(encode(new String[] {})).isEqualTo("0x00000000");
  }

  @Test
  public void should_encode_non_empty_array() {
    when(elementCodec.encode("hello", ProtocolVersion.DEFAULT))
        .thenReturn(Bytes.fromHexString("0x68656c6c6f"));
    when(elementCodec.encode("world", ProtocolVersion.DEFAULT))
        .thenReturn(Bytes.fromHexString("0x776f726c64"));
    assertThat(encode(new String[] {"hello", "world"}))
        .isEqualTo(
            "0x"
                + "00000002" // number of elements
                + "00000005" // size of element 1
                + "68656c6c6f" // contents of element 1
                + "00000005" // size of element 2
                + "776f726c64" // contents of element 3
            );
  }

  @Test
  public void should_decode_null_as_empty_array() {
    assertThat(decode(null)).isEmpty();
  }

  @Test
  public void should_decode_empty_array() {
    assertThat(decode("0x00000000")).isEmpty();
  }

  @Test
  public void should_decode_non_empty_array() {
    when(elementCodec.decode(Bytes.fromHexString("0x68656c6c6f"), ProtocolVersion.DEFAULT))
        .thenReturn("hello");
    when(elementCodec.decode(Bytes.fromHexString("0x776f726c64"), ProtocolVersion.DEFAULT))
        .thenReturn("world");
    assertThat(
            decode(
                "0x"
                    + "00000002" // number of elements
                    + "00000005" // size of element 1
                    + "68656c6c6f" // contents of element 1
                    + "00000005" // size of element 2
                    + "776f726c64" // contents of element 3
                ))
        .containsExactly("hello", "world");
  }

  @Test
  public void should_decode_array_with_null_elements() {
    when(elementCodec.decode(Bytes.fromHexString("0x68656c6c6f"), ProtocolVersion.DEFAULT))
        .thenReturn("hello");
    assertThat(
            decode(
                "0x"
                    + "00000002" // number of elements
                    + "FFFFFFFF" // size of element 1 (-1 for null)
                    + "00000005" // size of element 2
                    + "68656c6c6f" // contents of element 2
                ))
        .containsExactly(null, "hello");
  }

  @Test
  public void should_format_null_array() {
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_format_empty_array() {
    assertThat(format(new String[] {})).isEqualTo("[]");
  }

  @Test
  public void should_format_non_empty_array() {
    when(elementCodec.format("hello")).thenReturn("'hello'");
    when(elementCodec.format("world")).thenReturn("'world'");
    assertThat(format(new String[] {"hello", "world"})).isEqualTo("['hello','world']");
  }

  @Test
  public void should_parse_null_or_empty_string() {
    assertThat(parse(null)).isNull();
    assertThat(parse("")).isNull();
  }

  @Test
  public void should_parse_empty_array() {
    assertThat(parse("[]")).isEmpty();
  }

  @Test
  public void should_parse_non_empty_array() {
    when(elementCodec.parse("'hello'")).thenReturn("hello");
    when(elementCodec.parse("'world'")).thenReturn("world");
    assertThat(parse("['hello','world']")).containsExactly("hello", "world");
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_malformed_array() {
    parse("not an array");
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.arrayOf(String.class))).isTrue();
    assertThat(codec.accepts(GenericType.arrayOf(Integer.class))).isFalse();
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(GenericType.arrayOf(String.class).getRawType())).isTrue();
    assertThat(codec.accepts(GenericType.arrayOf(Integer.class).getRawType())).isFalse();
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(new String[] {"hello", "world"})).isTrue();
    assertThat(codec.accepts(new Integer[] {1, 2, 3})).isFalse();
  }
}
