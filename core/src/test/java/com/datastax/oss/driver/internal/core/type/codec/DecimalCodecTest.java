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
import java.math.BigDecimal;
import org.junit.Test;

public class DecimalCodecTest extends CodecTestBase<BigDecimal> {

  public DecimalCodecTest() {
    this.codec = TypeCodecs.DECIMAL;
  }

  @Test
  public void should_encode() {
    assertThat(encode(BigDecimal.ONE))
        .isEqualTo(
            "0x"
                + "00000000" // scale
                + "01" // unscaled value
            );
    assertThat(encode(BigDecimal.valueOf(128, 4)))
        .isEqualTo(
            "0x"
                + "00000004" // scale
                + "0080" // unscaled value
            );
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode("0x0000000001")).isEqualTo(BigDecimal.ONE);
    assertThat(decode("0x000000040080")).isEqualTo(BigDecimal.valueOf(128, 4));
    assertThat(decode("0x")).isNull();
    assertThat(decode(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_not_enough_bytes() {
    decode("0x0000");
  }

  @Test
  public void should_format() {
    assertThat(format(BigDecimal.ONE)).isEqualTo("1");
    assertThat(format(BigDecimal.valueOf(128, 4))).isEqualTo("0.0128");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    assertThat(parse("1")).isEqualTo(BigDecimal.ONE);
    assertThat(parse("0.0128")).isEqualTo(BigDecimal.valueOf(128, 4));
    assertThat(parse("NULL")).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("")).isNull();
    assertThat(parse(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("not a decimal");
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.of(BigDecimal.class))).isTrue();
    assertThat(codec.accepts(GenericType.of(Integer.class))).isFalse();
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(BigDecimal.class)).isTrue();
    assertThat(codec.accepts(Integer.class)).isFalse();
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(BigDecimal.ONE)).isTrue();
    assertThat(codec.accepts(Integer.MIN_VALUE)).isFalse();
  }
}
