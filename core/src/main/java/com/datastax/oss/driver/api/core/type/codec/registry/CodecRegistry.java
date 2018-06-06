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
package com.datastax.oss.driver.api.core.type.codec.registry;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.GettableByIndex;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import java.util.List;

/**
 * Provides codecs to convert CQL types to their Java equivalent, and vice-versa.
 *
 * <p>Implementations <b>MUST</b> provide a default mapping for all CQL types (primitive types, and
 * all the collections, tuples or user-defined types that can recursively be built from them &mdash;
 * see {@link DataTypes}).
 *
 * <p>They may also provide additional mappings to other Java types (for use with methods such as
 * {@link Row#get(int, Class)}, {@link TupleValue#set(int, Object, Class)}, etc.)
 */
public interface CodecRegistry {
  /**
   * An immutable instance, that only handles built-in driver types (that is, primitive types, and
   * collections, tuples, and user defined types thereof).
   */
  CodecRegistry DEFAULT = new DefaultCodecRegistry("default");

  /**
   * Returns a codec to handle the conversion between the given types.
   *
   * <p>This is used internally by the driver, in cases where both types are known, for example
   * {@link GettableByIndex#getString(int) row.getString(0)} (Java type inferred from the method,
   * CQL type known from the row metadata).
   *
   * <p>The driver's default registry implementation is <em>invariant</em> with regard to the Java
   * type: for example, if {@code B extends A} and an {@code A<=>int} codec is registered, {@code
   * codecFor(DataTypes.INT, B.class)} <b>will not</b> find that codec. This is because this method
   * is used internally both for encoding and decoding, and covariance wouldn't work when decoding.
   *
   * @throws CodecNotFoundException if there is no such codec.
   */
  <T> TypeCodec<T> codecFor(DataType cqlType, GenericType<T> javaType);

  /**
   * Shortcut for {@link #codecFor(DataType, GenericType) codecFor(cqlType,
   * GenericType.of(javaType))}.
   *
   * <p>Implementations may decide to override this method for performance reasons, if they have a
   * way to avoid the overhead of wrapping.
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
   * <p>This is used internally by the driver, in cases where the Java type is not explicitly
   * provided, for example {@link GettableByIndex#getObject(int) row.getObject(0)} (CQL type known
   * from the row metadata, Java type unspecified).
   *
   * <p>The definition of "most appropriate" is left to the appreciation of the registry
   * implementor.
   *
   * @throws CodecNotFoundException if there is no such codec.
   */
  <T> TypeCodec<T> codecFor(DataType cqlType);

  /**
   * Returns a codec to convert the given Java type to the CQL type deemed most appropriate to
   * represent it.
   *
   * <p>The driver does not use this method. It is provided as a convenience for third-party usage,
   * for example if you were to generate a schema based on a set of Java classes.
   *
   * <p>The driver's default registry implementation is <em>invariant</em> with regard to the Java
   * type: for example, if {@code B extends A} and an {@code A<=>int} codec is registered, {@code
   * codecFor(DataTypes.INT, B.class)} <b>will not</b> find that codec. This is because we don't
   * know whether this method will be used for encoding, decoding, or both.
   *
   * @throws CodecNotFoundException if there is no such codec.
   */
  <T> TypeCodec<T> codecFor(GenericType<T> javaType);

  /**
   * Shortcut for {@link #codecFor(GenericType) codecFor(GenericType.of(javaType))}.
   *
   * <p>Implementations may decide to override this method for performance reasons, if they have a
   * way to avoid the overhead of wrapping.
   *
   * @throws CodecNotFoundException if there is no such codec.
   */
  default <T> TypeCodec<T> codecFor(Class<T> javaType) {
    return codecFor(GenericType.of(javaType));
  }

  /**
   * Returns a codec to convert the given Java object to the given CQL type.
   *
   * <p>This is used internally by the driver when you bulk-set values in a {@link
   * PreparedStatement#bind(Object...) bound statement}, {@link UserDefinedType#newValue(Object...)
   * UDT} or {@link TupleType#newValue(Object...) tuple}.
   *
   * <p>Unlike other methods, the driver's default registry implementation is <em>covariant</em>
   * with regard to the Java type: for example, if {@code B extends A} and an {@code A<=>int} codec
   * is registered, {@code codecFor(DataTypes.INT, someB)} <b>will</b> find that codec. This is
   * because this method is always used in encoding scenarios; if a bound statement has a value with
   * a runtime type of {@code ArrayList<String>}, it should be possible to encode it with a codec
   * that accepts a {@code List<String>}.
   *
   * @throws CodecNotFoundException if there is no such codec.
   */
  <T> TypeCodec<T> codecFor(DataType cqlType, T value);

  /**
   * Returns a codec to convert the given Java object to the CQL type deemed most appropriate to
   * represent it.
   *
   * <p>This is used internally by the driver, in cases where the CQL type is unknown, for example
   * for {@linkplain SimpleStatement#setPositionalValues(List) simple statement variables} (simple
   * statements don't have access to schema metadata).
   *
   * <p>Unlike other methods, the driver's default registry implementation is <em>covariant</em>
   * with regard to the Java type: for example, if {@code B extends A} and an {@code A<=>int} codec
   * is registered, {@code codecFor(someB)} <b>will</b> find that codec. This is because this method
   * is always used in encoding scenarios; if a simple statement has a value with a runtime type of
   * {@code ArrayList<String>}, it should be possible to encode it with a codec that accepts a
   * {@code List<String>}.
   *
   * @throws CodecNotFoundException if there is no such codec.
   */
  <T> TypeCodec<T> codecFor(T value);
}
