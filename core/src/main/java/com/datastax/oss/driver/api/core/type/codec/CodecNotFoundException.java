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
package com.datastax.oss.driver.api.core.type.codec;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/** Thrown when a suitable {@link TypeCodec} cannot be found by the {@link CodecRegistry}. */
public class CodecNotFoundException extends RuntimeException {

  private final DataType cqlType;

  private final GenericType<?> javaType;

  public CodecNotFoundException(@Nullable DataType cqlType, @Nullable GenericType<?> javaType) {
    this(
        String.format("Codec not found for requested operation: [%s <-> %s]", cqlType, javaType),
        null,
        cqlType,
        javaType);
  }

  public CodecNotFoundException(
      @NonNull Throwable cause, @Nullable DataType cqlType, @Nullable GenericType<?> javaType) {
    this(
        String.format(
            "Error while looking up codec for requested operation: [%s <-> %s]", cqlType, javaType),
        cause,
        cqlType,
        javaType);
  }

  private CodecNotFoundException(
      String msg, Throwable cause, DataType cqlType, GenericType<?> javaType) {
    super(msg, cause);
    this.cqlType = cqlType;
    this.javaType = javaType;
  }

  @Nullable
  public DataType getCqlType() {
    return cqlType;
  }

  @Nullable
  public GenericType<?> getJavaType() {
    return javaType;
  }
}
