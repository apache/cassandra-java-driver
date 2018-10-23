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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.util.UUID;
import org.junit.Test;

public class UuidCodecTest extends CodecTestBase<UUID> {
  private static final UUID MOCK_UUID = new UUID(2L, 1L);

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

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_not_enough_bytes() {
    decode("0x0000");
  }

  @Test(expected = IllegalArgumentException.class)
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

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("not a uuid");
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.of(UUID.class))).isTrue();
    assertThat(codec.accepts(GenericType.of(Integer.class))).isFalse();
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(UUID.class)).isTrue();
    assertThat(codec.accepts(Integer.class)).isFalse();
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(MOCK_UUID)).isTrue();
    assertThat(codec.accepts(Integer.MIN_VALUE)).isFalse();
  }
}
