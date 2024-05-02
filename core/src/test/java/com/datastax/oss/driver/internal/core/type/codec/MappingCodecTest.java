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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.junit.Test;

public class MappingCodecTest extends CodecTestBase<String> {

  public MappingCodecTest() {
    this.codec = new CqlIntToStringCodec();
  }

  @Test
  public void should_encode() {
    // Our codec relies on the JDK's ByteBuffer API. We're not testing the JDK, so no need to try
    // a thousand different values.
    assertThat(encode("0")).isEqualTo("0x00000000");
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode("0x00000000")).isEqualTo("0");
    assertThat(decode("0x")).isNull();
    assertThat(decode(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_not_enough_bytes() {
    decode("0x0000");
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_too_many_bytes() {
    decode("0x0000000000000000");
  }

  @Test
  public void should_format() {
    assertThat(format("0")).isEqualTo("0");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    assertThat(parse("0")).isEqualTo("0");
    assertThat(parse("NULL")).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("")).isNull();
    assertThat(parse(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("not an int");
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.of(String.class))).isTrue();
    assertThat(codec.accepts(GenericType.of(int.class))).isFalse();
    assertThat(codec.accepts(GenericType.of(Integer.class))).isFalse();
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(String.class)).isTrue();
    assertThat(codec.accepts(int.class)).isFalse();
    assertThat(codec.accepts(Integer.class)).isFalse();
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts("123")).isTrue();
    // codec accepts any String, even if it can't be encoded
    assertThat(codec.accepts("not an int")).isTrue();
    assertThat(codec.accepts(Integer.MIN_VALUE)).isFalse();
  }

  @Test
  public void should_expose_inner_and_outer_java_types() {
    assertThat(((MappingCodec<?, ?>) codec).getInnerJavaType()).isEqualTo(GenericType.INTEGER);
    assertThat(codec.getJavaType()).isEqualTo(GenericType.STRING);
  }
}
