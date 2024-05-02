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
package com.datastax.oss.driver.internal.core.type.codec.extras.enums;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.CodecTestBase;
import org.junit.Before;
import org.junit.Test;

public class EnumNameCodecTest extends CodecTestBase<DefaultProtocolVersion> {

  @Before
  public void setup() {
    codec = ExtraTypeCodecs.enumNamesOf(DefaultProtocolVersion.class);
  }

  @Test
  public void should_encode() {
    // Our codec relies on the JDK's ByteBuffer API. We're not testing the JDK, so no need to try
    // a thousand different values.
    assertThat(encode(DefaultProtocolVersion.V3)).isEqualTo("0x5633");
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode("0x5633")).isEqualTo(DefaultProtocolVersion.V3);
    assertThat(decode("0x")).isNull();
    assertThat(decode(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_invalid_name() {
    decode("0x1234");
  }

  @Test
  public void should_format() {
    assertThat(format(DefaultProtocolVersion.V3)).isEqualTo("'V3'");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    assertThat(parse("'V3'")).isEqualTo(DefaultProtocolVersion.V3);
    assertThat(parse("NULL")).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("")).isNull();
    assertThat(parse(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("not a valid enum constant");
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.of(DefaultProtocolVersion.class))).isTrue();
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(DefaultProtocolVersion.class)).isTrue();
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(DefaultProtocolVersion.V3)).isTrue();
    assertThat(codec.accepts(DseProtocolVersion.DSE_V1)).isFalse();
  }
}
