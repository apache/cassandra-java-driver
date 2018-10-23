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
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.Test;

public class InetCodecTest extends CodecTestBase<InetAddress> {

  private static final InetAddress V4_ADDRESS;
  private static final InetAddress V6_ADDRESS;

  static {
    try {
      V4_ADDRESS = InetAddress.getByName("127.0.0.1");
      V6_ADDRESS = InetAddress.getByName("::1");
    } catch (UnknownHostException e) {
      fail("unexpected error", e);
      throw new AssertionError(); // never reached
    }
  }

  public InetCodecTest() {
    this.codec = TypeCodecs.INET;
  }

  @Test
  public void should_encode() {
    assertThat(encode(V4_ADDRESS)).isEqualTo("0x7f000001");
    assertThat(encode(V6_ADDRESS)).isEqualTo("0x00000000000000000000000000000001");
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode("0x7f000001")).isEqualTo(V4_ADDRESS);
    assertThat(decode("0x00000000000000000000000000000001")).isEqualTo(V6_ADDRESS);
    assertThat(decode("0x")).isNull();
    assertThat(decode(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_not_enough_bytes() {
    decode("0x0000");
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_incorrect_byte_count() {
    decode("0x" + Strings.repeat("00", 7));
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_decode_if_too_many_bytes() {
    decode("0x" + Strings.repeat("00", 17));
  }

  @Test
  public void should_format() {
    assertThat(format(V4_ADDRESS)).isEqualTo("'127.0.0.1'");
    assertThat(format(V6_ADDRESS)).isEqualTo("'0:0:0:0:0:0:0:1'");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    assertThat(parse("'127.0.0.1'")).isEqualTo(V4_ADDRESS);
    assertThat(parse("'0:0:0:0:0:0:0:1'")).isEqualTo(V6_ADDRESS);
    assertThat(parse("NULL")).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("")).isNull();
    assertThat(parse(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("not an address");
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.of(InetAddress.class))).isTrue();
    assertThat(codec.accepts(GenericType.of(Inet4Address.class)))
        .isFalse(); // covariance not allowed
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(InetAddress.class)).isTrue();
    assertThat(codec.accepts(Inet4Address.class)).isFalse(); // covariance not allowed
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(V4_ADDRESS)).isTrue(); // covariance allowed
    assertThat(codec.accepts(V6_ADDRESS)).isTrue(); // covariance allowed
  }
}
