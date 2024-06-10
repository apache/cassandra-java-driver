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

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import org.junit.Test;

public class TimeCodecTest extends CodecTestBase<LocalTime> {

  public TimeCodecTest() {
    this.codec = TypeCodecs.TIME;
  }

  @Test
  public void should_encode() {
    assertThat(encode(LocalTime.MIDNIGHT)).isEqualTo("0x0000000000000000");
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode("0x0000000000000000")).isEqualTo(LocalTime.MIDNIGHT);
    assertThat(decode(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_not_enough_bytes() {
    decode("0x0000");
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_too_many_bytes() {
    decode("0x0000000000000000" + "0000");
  }

  @Test
  public void should_format() {
    // No need to test various values because the codec delegates directly to the JDK's formatter,
    // which we assume does its job correctly.
    assertThat(format(LocalTime.MIDNIGHT)).isEqualTo("'00:00:00.000000000'");
    assertThat(format(LocalTime.NOON.plus(13799999994L, ChronoUnit.NANOS)))
        .isEqualTo("'12:00:13.799999994'");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    // Raw number
    assertThat(parse("'0'")).isEqualTo(LocalTime.MIDNIGHT);

    // String format
    assertThat(parse("'00:00'")).isEqualTo(LocalTime.MIDNIGHT);

    assertThat(parse("NULL")).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("")).isNull();
    assertThat(parse(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("not a time");
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.of(LocalTime.class))).isTrue();
    assertThat(codec.accepts(GenericType.of(Integer.class))).isFalse();
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(LocalTime.class)).isTrue();
    assertThat(codec.accepts(Integer.class)).isFalse();
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(LocalTime.MIDNIGHT)).isTrue();
    assertThat(codec.accepts(Integer.MIN_VALUE)).isFalse();
  }
}
