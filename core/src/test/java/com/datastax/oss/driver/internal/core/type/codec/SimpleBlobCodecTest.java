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

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import org.junit.Test;

public class SimpleBlobCodecTest extends CodecTestBase<byte[]> {

  private static final ByteBuffer BUFFER = Bytes.fromHexString("0xcafebabe");
  private static final byte[] ARRAY = Bytes.getArray(Bytes.fromHexString("0xcafebabe"));

  public SimpleBlobCodecTest() {
    this.codec = ExtraTypeCodecs.BLOB_TO_ARRAY;
  }

  @Test
  public void should_encode() {
    assertThat(encode(ARRAY)).isEqualTo("0xcafebabe");
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_not_share_position_between_input_and_encoded() {
    ByteBuffer encoded = codec.encode(ARRAY, ProtocolVersion.DEFAULT);
    assertThat(encoded).isNotNull();
    assertThat(ARRAY).isEqualTo(Bytes.getArray(encoded));
  }

  @Test
  public void should_decode() {
    assertThat(decode("0xcafebabe")).isEqualTo(ARRAY);
    assertThat(decode("0x")).hasSize(0);
    assertThat(decode(null)).isNull();
  }

  @Test
  public void should_not_share_position_between_decoded_and_input() {
    byte[] decoded = codec.decode(BUFFER, ProtocolVersion.DEFAULT);
    assertThat(decoded).isEqualTo(ARRAY);
  }

  @Test
  public void should_format() {
    assertThat(format(ARRAY)).isEqualTo("0xcafebabe");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    assertThat(parse("0xcafebabe")).isEqualTo(ARRAY);
    assertThat(parse("NULL")).isNull();
    assertThat(parse("null")).isNull();
    assertThat(parse("")).isNull();
    assertThat(parse(null)).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_input() {
    parse("not a blob");
  }

  @Test
  public void should_accept_generic_type() {
    assertThat(codec.accepts(GenericType.of(byte[].class))).isTrue();
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(byte[].class)).isTrue();
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(ARRAY)).isTrue();
  }
}
