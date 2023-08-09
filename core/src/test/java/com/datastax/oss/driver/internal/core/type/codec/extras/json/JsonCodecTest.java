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
package com.datastax.oss.driver.internal.core.type.codec.extras.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.CodecTestBase;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class JsonCodecTest extends CodecTestBase<Set<InetAddress>> {

  private static final InetAddress V4_ADDRESS;
  private static final InetAddress V6_ADDRESS;
  private static final Set<InetAddress> SET_OF_ADDRESSES;

  static {
    try {
      V4_ADDRESS = InetAddress.getByName("127.0.0.1");
      V6_ADDRESS = InetAddress.getByName("::1");
      SET_OF_ADDRESSES = ImmutableSet.of(V4_ADDRESS, V6_ADDRESS);
    } catch (UnknownHostException e) {
      fail("unexpected error", e);
      throw new AssertionError(); // never reached
    }
  }

  @Before
  public void setup() {
    this.codec = ExtraTypeCodecs.json(GenericType.setOf(GenericType.INET_ADDRESS));
  }

  @Test
  public void should_encode() {
    assertThat(encode(SET_OF_ADDRESSES))
        .isEqualTo(encodeJson("[\"127.0.0.1\",\"0:0:0:0:0:0:0:1\"]"));
    assertThat(encode(Collections.emptySet())).isEqualTo(encodeJson("[]"));
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode(encodeJson("[\"127.0.0.1\",\"0:0:0:0:0:0:0:1\"]")))
        .isEqualTo(SET_OF_ADDRESSES);
    assertThat(decode(encodeJson("[]"))).isEqualTo(Collections.emptySet());
    assertThat(decode(null)).isNull();
  }

  @Test
  public void should_format() {
    assertThat(format(SET_OF_ADDRESSES)).isEqualTo("'[\"127.0.0.1\",\"0:0:0:0:0:0:0:1\"]'");
    assertThat(format(Collections.emptySet())).isEqualTo("'[]'");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    assertThat(parse("'[\"127.0.0.1\",\"0:0:0:0:0:0:0:1\"]'")).isEqualTo(SET_OF_ADDRESSES);
    assertThat(parse("'[]'")).isEqualTo(Collections.emptySet());
    assertThat(parse("NULL")).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("")).isNull();
    assertThat(parse(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("not a JSON string");
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.setOf(GenericType.INET_ADDRESS))).isTrue();
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(Set.class)).isTrue();
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(SET_OF_ADDRESSES)).isTrue();
    assertThat(codec.accepts(Collections.emptySet())).isTrue();
    assertThat(codec.accepts(Collections.singletonList(V4_ADDRESS))).isFalse();
  }

  private String encodeJson(String json) {
    return Bytes.toHexString(TypeCodecs.TEXT.encode(json, ProtocolVersion.DEFAULT));
  }
}
