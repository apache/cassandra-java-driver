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

import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.ByteListToArrayCodec;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import net.jcip.annotations.Immutable;

/**
 * A codec that maps the CQL type {@code blob} to the Java type {@code byte[]}.
 *
 * <p>If you are looking for a codec mapping the CQL type {@code blob} to the Java type {@link
 * ByteBuffer}, you should use {@link BlobCodec} instead.
 *
 * <p>If you are looking for a codec mapping the CQL type {@code list<tinyint} to the Java type
 * {@code byte[]}, you should use {@link ByteListToArrayCodec} instead.
 */
@Immutable
public class SimpleBlobCodec extends MappingCodec<ByteBuffer, byte[]> {

  public SimpleBlobCodec() {
    super(TypeCodecs.BLOB, GenericType.of(byte[].class));
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof byte[];
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return byte[].class.equals(javaClass);
  }

  @Nullable
  @Override
  protected byte[] innerToOuter(@Nullable ByteBuffer value) {
    return value == null ? null : ByteUtils.getArray(value);
  }

  @Nullable
  @Override
  protected ByteBuffer outerToInner(@Nullable byte[] value) {
    return value == null ? null : ByteBuffer.wrap(value);
  }
}
