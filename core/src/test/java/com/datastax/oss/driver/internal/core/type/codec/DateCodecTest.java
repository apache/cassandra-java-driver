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
import java.time.LocalDate;
import org.junit.Test;

public class DateCodecTest extends CodecTestBase<LocalDate> {

  private static final LocalDate EPOCH = LocalDate.ofEpochDay(0);
  private static final LocalDate MIN = LocalDate.parse("-5877641-06-23");
  private static final LocalDate MAX = LocalDate.parse("+5881580-07-11");

  public DateCodecTest() {
    this.codec = TypeCodecs.DATE;
  }

  @Test
  public void should_encode() {
    // Dates are encoded as a number of days since the epoch, stored on 8 bytes with 0 in the
    // middle.
    assertThat(encode(MIN)).isEqualTo("0x00000000");
    // The "middle" is the one that has only the most significant bit set (because it has the same
    // number of values before and after it, determined by all possible combinations of the
    // remaining bits)
    assertThat(encode(EPOCH)).isEqualTo("0x80000000");
    assertThat(encode(MAX)).isEqualTo("0xffffffff");
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode("0x00000000")).isEqualTo(MIN);
    assertThat(decode("0x80000000")).isEqualTo(EPOCH);
    assertThat(decode("0xffffffff")).isEqualTo(MAX);
    assertThat(decode(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_too_many_bytes() {
    decode("0x00000000" + "0000");
  }

  @Test
  public void should_format() {
    // No need to test various values because the codec delegates directly to the JDK's formatter,
    // which we assume does its job correctly.
    assertThat(format(EPOCH)).isEqualTo("'1970-01-01'");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    // Raw number
    assertThat(parse("0")).isEqualTo(MIN);
    assertThat(parse("2147483648")).isEqualTo(EPOCH);

    // Date format
    assertThat(parse("'-5877641-06-23'")).isEqualTo(MIN);
    assertThat(parse("'1970-01-01'")).isEqualTo(EPOCH);
    assertThat(parse("'2014-01-01'")).isEqualTo(LocalDate.parse("2014-01-01"));

    assertThat(parse("NULL")).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("")).isNull();
    assertThat(parse(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("not a date");
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.of(LocalDate.class))).isTrue();
    assertThat(codec.accepts(GenericType.of(Integer.class))).isFalse();
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(LocalDate.class)).isTrue();
    assertThat(codec.accepts(Integer.class)).isFalse();
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(EPOCH)).isTrue();
    assertThat(codec.accepts(Integer.MIN_VALUE)).isFalse();
  }
}
