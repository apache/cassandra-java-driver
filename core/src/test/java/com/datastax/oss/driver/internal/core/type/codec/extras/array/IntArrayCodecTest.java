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
package com.datastax.oss.driver.internal.core.type.codec.extras.array;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.CodecTestBase;
import org.junit.Before;
import org.junit.Test;

public class IntArrayCodecTest extends CodecTestBase<int[]> {

  @Before
  public void setup() {
    codec = ExtraTypeCodecs.INT_LIST_TO_ARRAY;
  }

  @Test
  public void should_encode_null() {
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_encode_empty_array() {
    assertThat(encode(new int[] {})).isEqualTo("0x00000000");
  }

  @Test
  public void should_encode_non_empty_array() {
    assertThat(encode(new int[] {1, 2, 3}))
        .isEqualTo(
            "0x"
                + "00000003" // number of elements
                + "00000004" // size of element 1
                + "00000001" // contents of element 1
                + "00000004" // size of element 2
                + "00000002" // contents of element 2
                + "00000004" // size of element 3
                + "00000003" // contents of element 3
            );
  }

  @Test
  public void should_decode_null_as_empty_array() {
    assertThat(decode(null)).isEmpty();
  }

  @Test
  public void should_decode_empty_array() {
    assertThat(decode("0x00000000")).isEmpty();
  }

  @Test
  public void should_decode_non_empty_array() {
    assertThat(
            decode(
                "0x"
                    + "00000003" // number of elements
                    + "00000004" // size of element 1
                    + "00000001" // contents of element 1
                    + "00000004" // size of element 2
                    + "00000002" // contents of element 2
                    + "00000004" // size of element 3
                    + "00000003" // contents of element 3
                ))
        .containsExactly(1, 2, 3);
  }

  @Test(expected = NullPointerException.class)
  public void should_not_decode_array_with_null_elements() {
    decode(
        "0x"
            + "00000001" // number of elements
            + "FFFFFFFF" // size of element 1 (-1 for null)
        );
  }

  @Test
  public void should_format_null_array() {
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_format_empty_array() {
    assertThat(format(new int[] {})).isEqualTo("[]");
  }

  @Test
  public void should_format_non_empty_array() {
    assertThat(format(new int[] {1, 2, 3})).isEqualTo("[1,2,3]");
  }

  @Test
  public void should_parse_null_or_empty_string() {
    assertThat(parse(null)).isNull();
    assertThat(parse("")).isNull();
  }

  @Test
  public void should_parse_empty_array() {
    assertThat(parse("[]")).isEmpty();
  }

  @Test
  public void should_parse_non_empty_array() {
    assertThat(parse("[1,2,3]")).containsExactly(1, 2, 3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_malformed_array() {
    parse("not an array");
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.arrayOf(Integer.TYPE))).isTrue();
    assertThat(codec.accepts(GenericType.arrayOf(Integer.class))).isFalse();
    assertThat(codec.accepts(GenericType.arrayOf(String.class))).isFalse();
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(GenericType.arrayOf(Integer.TYPE).getRawType())).isTrue();
    assertThat(codec.accepts(GenericType.arrayOf(Integer.class).getRawType())).isFalse();
    assertThat(codec.accepts(GenericType.arrayOf(String.class).getRawType())).isFalse();
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(new int[] {1, 2, 3})).isTrue();
    assertThat(codec.accepts(new Integer[] {1, 2, 3})).isFalse();
    assertThat(codec.accepts(new String[] {"hello", "world"})).isFalse();
  }
}
