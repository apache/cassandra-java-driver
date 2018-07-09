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
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;

public class CodecTestBase<T> {
  protected TypeCodec<T> codec;

  protected String encode(T t, ProtocolVersion protocolVersion) {
    assertThat(codec).as("Must set codec before calling this method").isNotNull();
    ByteBuffer bytes = codec.encode(t, protocolVersion);
    return (bytes == null) ? null : Bytes.toHexString(bytes);
  }

  protected String encode(T t) {
    return encode(t, ProtocolVersion.DEFAULT);
  }

  protected T decode(String hexString, ProtocolVersion protocolVersion) {
    assertThat(codec).as("Must set codec before calling this method").isNotNull();
    ByteBuffer bytes = (hexString == null) ? null : Bytes.fromHexString(hexString);
    // Decode twice, to assert that decode leaves the input buffer in its original state
    codec.decode(bytes, protocolVersion);
    return codec.decode(bytes, protocolVersion);
  }

  protected T decode(String hexString) {
    return decode(hexString, ProtocolVersion.DEFAULT);
  }

  protected String format(T t) {
    assertThat(codec).as("Must set codec before calling this method").isNotNull();
    return codec.format(t);
  }

  protected T parse(String s) {
    assertThat(codec).as("Must set codec before calling this method").isNotNull();
    return codec.parse(s);
  }
}
