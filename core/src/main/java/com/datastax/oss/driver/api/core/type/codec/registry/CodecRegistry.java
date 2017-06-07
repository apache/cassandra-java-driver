/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core.type.codec.registry;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;

public interface CodecRegistry {
  /**
   * An immutable instance, that only handles built-in driver types (that is, primitive types, and
   * collections, tuples, and user defined types thereof).
   */
  CodecRegistry DEFAULT = new DefaultCodecRegistry();

  /**
   * Returns a codec to handle the conversion between the given types.
   *
   * @throws CodecNotFoundException if there is no such codec.
   */
  <T> TypeCodec<T> codecFor(DataType cqlType, GenericType<T> javaType);

  /**
   * Returns a codec to handle the conversion between the given types.
   *
   * <p>The default implementation of this method simply wraps {@code javaType} and calls {@link
   * #codecFor(DataType, GenericType)}. Implementors can override it if they can avoid the overhead
   * of wrapping.
   *
   * @throws CodecNotFoundException if there is no such codec.
   */
  default <T> TypeCodec<T> codecFor(DataType cqlType, Class<T> javaType) {
    return codecFor(cqlType, GenericType.of(javaType));
  }

  /**
   * Returns a codec to convert the given CQL type to the Java type deemed most appropriate to
   * represent it.
   *
   * <p>The definition of "most appropriate" is unspecified, and left to the appreciation of the
   * registry implementor.
   */
  <T> TypeCodec<T> codecFor(DataType cqlType);

  /**
   * Returns a codec to convert the given Java object to the CQL type deemed most appropriate to
   * represent it.
   *
   * <p>The definition of "most appropriate" is unspecified, and left to the appreciation of the
   * registry implementor.
   */
  <T> TypeCodec<T> codecFor(T value);
}
