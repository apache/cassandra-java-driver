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

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.CqlVectorType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;

public class CqlVectorCodec implements TypeCodec<CqlVector> {

  @NonNull
  @Override
  public GenericType<CqlVector> getJavaType() {
    return GenericType.of(CqlVector.class);
  }

  /* Since we've overridden accepts() this shouldn't ever actually be used */
  @NonNull
  @Override
  public DataType getCqlType() {
    return new CqlVectorType(0);
  }

  @NonNull
  @Override
  public boolean accepts(@NonNull DataType cqlType) {
    Preconditions.checkNotNull(cqlType);
    return cqlType.getClass().equals(CqlVectorType.class);
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable CqlVector value, @NonNull ProtocolVersion protocolVersion) {
    float[] dimensions = value.getDimensions();
    ByteBuffer bytes = ByteBuffer.allocate(4 * dimensions.length);
    for (int i = 0; i < dimensions.length; ++i) bytes.putFloat(dimensions[i]);
    bytes.rewind();
    return bytes;
  }

  @Nullable
  @Override
  public CqlVector decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    int length = bytes.limit();
    if (length % 4 != 0)
      throw new IllegalArgumentException("Expected CqlVector to consist of a multiple of 4 bytes");
    int floatCnt = length / 4;
    float[] rv = new float[floatCnt];
    for (int i = 0; i < floatCnt; ++i) {
      rv[i] = bytes.getFloat();
    }
    return new CqlVector(rv);
  }

  @NonNull
  @Override
  public String format(@Nullable CqlVector value) {
    return null;
  }

  @Nullable
  @Override
  public CqlVector parse(@Nullable String value) {
    return null;
  }
}
