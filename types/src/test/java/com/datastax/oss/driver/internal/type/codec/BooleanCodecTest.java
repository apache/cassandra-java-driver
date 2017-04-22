/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.type.codec;

import com.datastax.oss.driver.api.type.codec.TypeCodecs;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BooleanCodecTest extends CodecTestBase<Boolean> {

  public BooleanCodecTest() {
    this.codec = TypeCodecs.BOOLEAN;
  }

  @Test
  public void should_encode() {
    assertThat(encode(false)).isEqualTo("0x00");
    assertThat(encode(true)).isEqualTo("0x01");
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode("0x00")).isFalse();
    assertThat(decode("0x01")).isTrue();
    assertThat(decode("0x")).isNull();
    assertThat(decode(null)).isNull();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void should_fail_to_decode_if_too_many_bytes() {
    decode("0x0000");
  }

  @Test
  public void should_format() {
    assertThat(format(true)).isEqualTo("true");
    assertThat(format(false)).isEqualTo("false");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    assertThat(parse("true")).isEqualTo(true);
    assertThat(parse("false")).isEqualTo(false);
    assertThat(parse("NULL")).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("")).isNull();
    assertThat(parse(null)).isNull();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("maybe");
  }
}
