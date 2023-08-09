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
import org.junit.Test;

public class AsciiCodecTest extends CodecTestBase<String> {
  public AsciiCodecTest() {
    this.codec = TypeCodecs.ASCII;
  }

  @Test
  public void should_encode() {
    assertThat(encode("hello")).isEqualTo("0x68656c6c6f");
    assertThat(encode(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_encode_non_ascii() {
    encode("hëllo");
  }

  @Test
  public void should_decode() {
    assertThat(decode("0x68656c6c6f")).isEqualTo("hello");
    assertThat(decode("0x")).isEmpty();
    assertThat(decode(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_non_ascii() {
    decode("0x68c3ab6c6c6f");
  }
}
