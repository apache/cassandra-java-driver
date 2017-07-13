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
package com.datastax.oss.driver.internal.core.type.codec;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StringCodecTest extends CodecTestBase<String> {

  public StringCodecTest() {
    // We don't test ASCII, since it only differs by the encoding used
    this.codec = TypeCodecs.TEXT;
  }

  @Test
  public void should_encode() {
    assertThat(encode("hello")).isEqualTo("0x68656c6c6f");
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode("0x68656c6c6f")).isEqualTo("hello");
    assertThat(decode("0x")).isEmpty();
    assertThat(decode(null)).isNull();
  }

  @Test
  public void should_format() {
    assertThat(format("hello")).isEqualTo("'hello'");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    assertThat(parse("'hello'")).isEqualTo("hello");
    assertThat(parse("NULL")).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("")).isNull();
    assertThat(parse(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("not a string");
  }
}
