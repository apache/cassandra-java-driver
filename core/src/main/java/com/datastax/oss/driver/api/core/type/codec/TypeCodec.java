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
package com.datastax.oss.driver.api.core.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import java.nio.ByteBuffer;

/** Manages the two-way conversion between a CQL type and a Java type. */
public interface TypeCodec<T> {

  GenericType<T> getJavaType();

  DataType getCqlType();

  /**
   * Whether this codec is capable of processing the given Java type.
   *
   * <p>The default implementation is <em>invariant</em> with respect to the passed argument
   * (through the usage of {@link GenericType#equals(Object)}) and <em>it's strongly recommended not
   * to modify this behavior</em>. This means that a codec will only ever accept the <em>exact</em>
   * Java type that it has been created for.
   *
   * <p>If the argument represents a Java primitive type, its wrapper type is considered instead.
   */
  default boolean accepts(GenericType<?> javaType) {
    Preconditions.checkNotNull(javaType);
    return getJavaType().__getToken().equals(javaType.__getToken().wrap());
  }

  /**
   * Whether this codec is capable of processing the given Java type.
   *
   * <p>The default implementation wraps the class in a generic type and calls {@link
   * #accepts(GenericType)}, therefore it is invariant as well.
   *
   * <p>Implementors are encouraged to override this method if there is a more efficient way. In
   * particular, if the codec targets a non-generic class, the check can be done with {@code
   * Class#isAssignableFrom}. Or even better, with {@code ==} if that class also happens to be
   * final.
   */
  default boolean accepts(Class<?> javaClass) {
    Preconditions.checkNotNull(javaClass);
    return accepts(GenericType.of(javaClass));
  }

  /**
   * Whether this codec is capable of encoding the given Java object.
   *
   * <p>The object's Java type is inferred from its runtime (raw) type, contrary to {@link
   * #accepts(GenericType)} which is capable of handling generic types.
   *
   * <p>The default implementation is <em>covariant</em> with respect to the passed argument and
   * <em>it's strongly recommended not to modify this behavior</em>. This means that, by default, a
   * codec will accept <em>any subtype</em> of the Java type that it has been created for.
   *
   * <p>It can only handle non-parameterized types; codecs handling parameterized types, such as
   * collection types, must override this method and perform some sort of "manual" inspection of the
   * actual type parameters.
   *
   * <p>Similarly, codecs that only accept a partial subset of all possible values must override
   * this method and manually inspect the object to check if it complies or not with the codec's
   * limitations.
   *
   * <p>Finally, if the codec targets a non-generic Java class, it might be possible to implement
   * this method with a simple {@code instanceof} check.
   */
  default boolean accepts(Object value) {
    Preconditions.checkNotNull(value);
    return getJavaType().__getToken().isSupertypeOf(TypeToken.of(value.getClass()));
  }

  /** Whether this codec is capable of processing the given CQL type. */
  default boolean accepts(DataType cqlType) {
    Preconditions.checkNotNull(cqlType);
    return this.getCqlType().equals(cqlType);
  }

  ByteBuffer encode(T value, ProtocolVersion protocolVersion);

  T decode(ByteBuffer bytes, ProtocolVersion protocolVersion);

  String format(T value);

  T parse(String value);
}
