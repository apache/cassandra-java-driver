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
package com.datastax.oss.driver;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.util.Bytes;
import io.netty.buffer.ByteBuf;
import org.assertj.core.api.AbstractAssert;

public class ByteBufAssert extends AbstractAssert<ByteBufAssert, ByteBuf> {
  public ByteBufAssert(ByteBuf actual) {
    super(actual, ByteBufAssert.class);
  }

  public ByteBufAssert containsExactly(String hexString) {
    ByteBuf copy = actual.duplicate();
    byte[] expectedBytes = Bytes.fromHexString(hexString).array();
    byte[] actualBytes = new byte[expectedBytes.length];
    copy.readBytes(actualBytes);
    assertThat(actualBytes).containsExactly(expectedBytes);
    // And nothing more
    assertThat(copy.isReadable()).isFalse();
    return this;
  }
}
