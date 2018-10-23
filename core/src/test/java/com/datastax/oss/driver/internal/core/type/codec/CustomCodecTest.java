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
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import org.junit.Test;

public class CustomCodecTest extends CodecTestBase<ByteBuffer> {
  private static final ByteBuffer BUFFER = Bytes.fromHexString("0xcafebabe");

  public CustomCodecTest() {
    this.codec = TypeCodecs.custom(DataTypes.custom("com.test.MyClass"));
  }

  @Test
  public void should_encode() {
    assertThat(encode(BUFFER)).isEqualTo("0xcafebabe");
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_not_share_position_between_input_and_encoded() {
    int inputPosition = BUFFER.position();
    ByteBuffer encoded = codec.encode(BUFFER, ProtocolVersion.DEFAULT);
    // Read from the encoded buffer to change its position
    encoded.get();
    // The input buffer should not be affected
    assertThat(BUFFER.position()).isEqualTo(inputPosition);
  }

  @Test
  public void should_decode() {
    assertThat(decode("0xcafebabe")).isEqualTo(BUFFER);
    assertThat(decode("0x").capacity()).isEqualTo(0);
    assertThat(decode(null)).isNull();
  }

  @Test
  public void should_not_share_position_between_decoded_and_input() {
    int inputPosition = BUFFER.position();
    ByteBuffer decoded = codec.decode(BUFFER, ProtocolVersion.DEFAULT);
    // Read from the decoded buffer to change its position
    decoded.get();
    // The input buffer should not be affected
    assertThat(BUFFER.position()).isEqualTo(inputPosition);
  }

  @Test
  public void should_format() {
    assertThat(format(BUFFER)).isEqualTo("0xcafebabe");
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    assertThat(parse("0xcafebabe")).isEqualTo(BUFFER);
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
    assertThat(codec.accepts(GenericType.of(ByteBuffer.class))).isTrue();
    assertThat(codec.accepts(GenericType.of(MappedByteBuffer.class)))
        .isFalse(); // covariance not allowed
  }

  @Test
  public void should_accept_raw_type() {
    assertThat(codec.accepts(ByteBuffer.class)).isTrue();
    assertThat(codec.accepts(MappedByteBuffer.class)).isFalse(); // covariance not allowed
  }

  @Test
  public void should_accept_object() {
    assertThat(codec.accepts(BUFFER)).isTrue();
    assertThat(codec.accepts(MappedByteBuffer.allocate(0))).isTrue(); // covariance allowed
  }
}
