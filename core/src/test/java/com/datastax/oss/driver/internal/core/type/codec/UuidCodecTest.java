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
import java.util.UUID;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class UuidCodecTest extends CodecTestBase<UUID> {
  private final UUID MOCK_UUID = new UUID(2L, 1L);

  public UuidCodecTest() {
    this.codec = TypeCodecs.UUID;
  }

  @Test
  public void should_encode() {
    assertThat(encode(MOCK_UUID)).isEqualTo("0x00000000000000020000000000000001");
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    UUID decoded = decode("0x00000000000000020000000000000001");
    assertThat(decoded.getMostSignificantBits()).isEqualTo(2L);
    assertThat(decoded.getLeastSignificantBits()).isEqualTo(1L);

    assertThat(decode(null)).isNull();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void should_fail_to_decode_if_not_enough_bytes() {
    decode("0x0000");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void should_fail_to_decode_if_too_many_bytes() {
    decode("0x00000000000000020000000000000001" + "0000");
  }

  @Test
  public void should_format() {
    assertThat(format(MOCK_UUID)).isEqualTo("00000000-0000-0002-0000-000000000001");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    assertThat(parse("00000000-0000-0002-0000-000000000001")).isEqualTo(MOCK_UUID);
    assertThat(parse("NULL")).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("")).isNull();
    assertThat(parse(null)).isNull();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("not a uuid");
  }
}
