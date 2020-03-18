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
package com.datastax.oss.driver.internal.core.type.codec.extras;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.CodecTestBase;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class OptionalCodecTest extends CodecTestBase<Optional<Integer>> {

  @Before
  public void setup() {
    codec = ExtraTypeCodecs.optionalOf(TypeCodecs.INT);
  }

  @Test
  public void should_encode() {
    // Our codec relies on the JDK's ByteBuffer API. We're not testing the JDK, so no need to try
    // a thousand different values.
    assertThat(encode(Optional.of(1))).isEqualTo("0x00000001");
    assertThat(encode(Optional.empty())).isNull();
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode("0x00000001")).isPresent().contains(1);
    assertThat(decode("0x")).isEmpty();
    assertThat(decode(null)).isEmpty();
  }

  @Test
  public void should_format() {
    assertThat(format(Optional.of(1))).isEqualTo("1");
    assertThat(format(Optional.empty())).isEqualTo("NULL");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    assertThat(parse("1")).isPresent().contains(1);
    assertThat(parse("NULL")).isEmpty();
    assertThat(parse("null")).isEmpty();
    assertThat(parse("")).isEmpty();
    assertThat(parse(null)).isEmpty();
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.optionalOf(Integer.class))).isTrue();
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(Optional.class)).isTrue();
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(Optional.of(1))).isTrue();
    assertThat(codec.accepts(Optional.empty())).isTrue();
    assertThat(codec.accepts(Optional.of("foo"))).isFalse();
  }
}
