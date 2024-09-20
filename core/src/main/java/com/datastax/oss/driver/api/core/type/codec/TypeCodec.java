/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;

/**
 * Manages the two-way conversion between a CQL type and a Java type.
 *
 * <p>Type codec implementations:
 *
 * <ol>
 *   <li><em>must</em> be thread-safe.
 *   <li><em>must</em> perform fast and never block.
 *   <li><em>must</em> support all native protocol versions; it is not possible to use different
 *       codecs for the same types but under different protocol versions.
 *   <li><em>must</em> comply with the native protocol specifications; failing to do so will result
 *       in unexpected results and could cause the driver to crash.
 *   <li><em>should</em> be stateless and immutable.
 *   <li><em>should</em> interpret {@code null} values and empty byte buffers (i.e. <code>
 *       {@link ByteBuffer#remaining()} == 0</code>) in a <em>reasonable</em> way; usually, {@code
 *       NULL} CQL values should map to {@code null} references, but exceptions exist; e.g. for
 *       varchar types, a {@code NULL} CQL value maps to a {@code null} reference, whereas an empty
 *       buffer maps to an empty String. For collection types, it is also admitted that {@code NULL}
 *       CQL values map to empty Java collections instead of {@code null} references. In any case,
 *       the codec's behavior with respect to {@code null} values and empty ByteBuffers should be
 *       clearly documented.
 *   <li>for Java types that have a primitive equivalent, <em>should</em> implement the appropriate
 *       "primitive" codec interface, e.g. {@link PrimitiveBooleanCodec} for {@code boolean}. This
 *       allows the driver to avoid the overhead of boxing when using primitive accessors such as
 *       {@link Row#getBoolean(int)}.
 *   <li>when decoding, <em>must</em> not consume {@link ByteBuffer} instances by performing
 *       relative read operations that modify their current position; codecs should instead prefer
 *       absolute read methods or, if necessary, {@link ByteBuffer#duplicate() duplicate} their byte
 *       buffers prior to reading them.
 * </ol>
 */
public interface TypeCodec<JavaTypeT> {

  @NonNull
  GenericType<JavaTypeT> getJavaType();

  @NonNull
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
  default boolean accepts(@NonNull GenericType<?> javaType) {
    Preconditions.checkNotNull(javaType);
    return getJavaType().equals(javaType.wrap());
  }

  /**
   * Whether this codec is capable of processing the given Java class.
   *
   * <p>This implementation simply compares the given class (or its wrapper type if it is a
   * primitive type) against this codec's runtime (raw) class; it is <em>invariant</em> with respect
   * to the passed argument (through the usage of {@link Class#equals(Object)} and <em>it's strongly
   * recommended not to modify this behavior</em>. This means that a codec will only ever return
   * {@code true} for the <em>exact</em> runtime (raw) Java class that it has been created for.
   *
   * <p>Implementors are encouraged to override this method if there is a more efficient way. In
   * particular, if the codec targets a final class, the check can be done with a simple {@code ==}.
   */
  default boolean accepts(@NonNull Class<?> javaClass) {
    Preconditions.checkNotNull(javaClass);
    if (javaClass.isPrimitive()) {
      if (javaClass == Boolean.TYPE) {
        javaClass = Boolean.class;
      } else if (javaClass == Character.TYPE) {
        javaClass = Character.class;
      } else if (javaClass == Byte.TYPE) {
        javaClass = Byte.class;
      } else if (javaClass == Short.TYPE) {
        javaClass = Short.class;
      } else if (javaClass == Integer.TYPE) {
        javaClass = Integer.class;
      } else if (javaClass == Long.TYPE) {
        javaClass = Long.class;
      } else if (javaClass == Float.TYPE) {
        javaClass = Float.class;
      } else if (javaClass == Double.TYPE) {
        javaClass = Double.class;
      }
    }
    return getJavaType().getRawType().equals(javaClass);
  }

  /**
   * Whether this codec is capable of encoding the given Java object.
   *
   * <p>The object's Java type is inferred from its runtime (raw) type, contrary to {@link
   * #accepts(GenericType)} which is capable of handling generic types.
   *
   * <p>Contrary to other {@code accept} methods, this method's default implementation is
   * <em>covariant</em> with respect to the passed argument (through the usage of {@link
   * Class#isAssignableFrom(Class)}) and <em>it's strongly recommended not to modify this
   * behavior</em>. This means that, by default, a codec will accept <em>any subtype</em> of the
   * Java type that it has been created for. This is so because codec lookups by arbitrary Java
   * objects only make sense when attempting to encode, never when attempting to decode, and indeed
   * the {@linkplain #encode(Object, ProtocolVersion) encode} method is covariant with {@code
   * JavaTypeT}.
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
  default boolean accepts(@NonNull Object value) {
    Preconditions.checkNotNull(value);
    return getJavaType().getRawType().isAssignableFrom(value.getClass());
  }

  /** Whether this codec is capable of processing the given CQL type. */
  default boolean accepts(@NonNull DataType cqlType) {
    Preconditions.checkNotNull(cqlType);
    return this.getCqlType().equals(cqlType);
  }

  /**
   * Encodes the given value in the binary format of the CQL type handled by this codec.
   *
   * <ul>
   *   <li>Null values should be gracefully handled and no exception should be raised; they should
   *       be considered as the equivalent of a NULL CQL value;
   *   <li>Codecs for CQL collection types should not permit null elements;
   *   <li>Codecs for CQL collection types should treat a {@code null} input as the equivalent of an
   *       empty collection.
   * </ul>
   */
  @Nullable
  ByteBuffer encode(@Nullable JavaTypeT value, @NonNull ProtocolVersion protocolVersion);

  /**
   * Decodes a value from the binary format of the CQL type handled by this codec.
   *
   * <ul>
   *   <li>Null or empty buffers should be gracefully handled and no exception should be raised;
   *       they should be considered as the equivalent of a NULL CQL value and, in most cases,
   *       should map to {@code null} or a default value for the corresponding Java type, if
   *       applicable;
   *   <li>Codecs for CQL collection types should clearly document whether they return immutable
   *       collections or not (note that the driver's default collection codecs return
   *       <em>mutable</em> collections);
   *   <li>Codecs for CQL collection types should avoid returning {@code null}; they should return
   *       empty collections instead (the driver's default collection codecs all comply with this
   *       rule);
   *   <li>The provided {@link ByteBuffer} should never be consumed by read operations that modify
   *       its current position; if necessary, {@link ByteBuffer#duplicate()} duplicate} it before
   *       consuming.
   * </ul>
   */
  @Nullable
  JavaTypeT decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion);

  /**
   * Formats the given value as a valid CQL literal according to the CQL type handled by this codec.
   *
   * <p>Implementors should take care of quoting and escaping the resulting CQL literal where
   * applicable. Null values should be accepted; in most cases, implementations should return the
   * CQL keyword {@code "NULL"} for {@code null} inputs.
   *
   * <p>Implementing this method is not strictly mandatory. It is used:
   *
   * <ol>
   *   <li>by the request logger, if parameter logging is enabled;
   *   <li>to format the INITCOND in {@link AggregateMetadata#describe(boolean)};
   *   <li>in the {@code toString()} representation of some driver objects (such as {@link UdtValue}
   *       and {@link TupleValue}), which is only used in driver logs;
   *   <li>for literal values in the query builder (see {@code QueryBuilder#literal(Object,
   *       CodecRegistry)} and {@code QueryBuilder#literal(Object, TypeCodec)}).
   * </ol>
   *
   * If you choose not to implement this method, don't throw an exception but instead return a
   * constant string (for example "XxxCodec.format not implemented").
   */
  @NonNull
  String format(@Nullable JavaTypeT value);

  /**
   * Formats items from collection type as valid list of CQL literals.
   *
   * <p>Implementing this method is not strictly mandatory. Default implementation falls back to
   * {@link #format(JavaTypeT)}. Method is used primarily for literal values in the query builder
   * (see {@code DefaultLiteral#appendElementsTo(StringBuilder)}.
   */
  default String formatElements(@Nullable JavaTypeT value) {
    return format(value);
  }

  /**
   * Parse the given CQL literal into an instance of the Java type handled by this codec.
   *
   * <p>Implementors should take care of unquoting and unescaping the given CQL string where
   * applicable. Null values and empty strings should be accepted, as well as the string {@code
   * "NULL"}; in most cases, implementations should interpret these inputs has equivalent to a
   * {@code null} reference.
   *
   * <p>Implementing this method is not strictly mandatory: internally, the driver only uses it to
   * parse the INITCOND when building the {@link AggregateMetadata metadata of an aggregate
   * function} (and in most cases it will use a built-in codec, unless the INITCOND has a custom
   * type).
   *
   * <p>If you choose not to implement this method, don't throw an exception but instead return
   * {@code null}.
   */
  @Nullable
  JavaTypeT parse(@Nullable String value);
}
