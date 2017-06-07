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
import java.time.Instant;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TimestampCodecTest extends CodecTestBase<Instant> {

  public TimestampCodecTest() {
    this.codec = TypeCodecs.TIMESTAMP;
  }

  @Test
  public void should_encode() {
    assertThat(encode(Instant.EPOCH)).isEqualTo("0x0000000000000000");
    assertThat(encode(Instant.ofEpochMilli(128))).isEqualTo("0x0000000000000080");
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode("0x0000000000000000").toEpochMilli()).isEqualTo(0);
    assertThat(decode("0x0000000000000080").toEpochMilli()).isEqualTo(128);
    assertThat(decode(null)).isNull();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void should_fail_to_decode_if_not_enough_bytes() {
    decode("0x0000");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void should_fail_to_decode_if_too_many_bytes() {
    decode("0x0000000000000000" + "0000");
  }

  @Test
  public void should_format() {
    // No need to test various values because the codec delegates directly to the JDK's formatter,
    // which we assume does its job correctly.
    assertThat(format(Instant.EPOCH)).isEqualTo("'1970-01-01T00:00:00.000+00:00'");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    // Raw number
    assertThat(parse("'0'")).isEqualTo(Instant.EPOCH);

    // Date format
    assertThat(parse("'1970-01-01T00:00Z'")).isEqualTo(Instant.EPOCH);

    assertThat(parse("NULL")).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("")).isNull();
    assertThat(parse(null)).isNull();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("not a timestamp");
  }
}
