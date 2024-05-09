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
package com.datastax.oss.driver.api.core.type.reflect;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.data.GettableByIndex;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.shaded.guava.common.primitives.Primitives;
import com.datastax.oss.driver.shaded.guava.common.reflect.TypeParameter;
import com.datastax.oss.driver.shaded.guava.common.reflect.TypeResolver;
import com.datastax.oss.driver.shaded.guava.common.reflect.TypeToken;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import net.jcip.annotations.Immutable;

/**
 * Runtime representation of a generic Java type.
 *
 * <p>This is used by type codecs to indicate which Java types they accept ({@link
 * TypeCodec#accepts(GenericType)}), and by generic getters and setters (such as {@link
 * GettableByIndex#get(int, GenericType)} in the driver's query API.
 *
 * <p>There are various ways to build instances of this class:
 *
 * <p>By using one of the static factory methods:
 *
 * <pre>{@code
 * GenericType<List<String>> stringListType = GenericType.listOf(String.class);
 * }</pre>
 *
 * By using an anonymous class:
 *
 * <pre>{@code
 * GenericType<Foo<Bar>> fooBarType = new GenericType<Foo<Bar>>(){};
 * }</pre>
 *
 * In a generic method, by using {@link #where(GenericTypeParameter, GenericType)} to substitute
 * free type variables with runtime types:
 *
 * <pre>{@code
 * <T> GenericType<Optional<T>> optionalOf(GenericType<T> elementType) {
 *   return new GenericType<Optional<T>>() {}
 *     .where(new GenericTypeParameter<T>() {}, elementType);
 * }
 * ...
 * GenericType<Optional<List<String>>> optionalStringListType = optionalOf(GenericType.listOf(String.class));
 * }</pre>
 *
 * <p>You are encouraged to store and reuse these instances.
 *
 * <p>Note that this class is a thin wrapper around Guava's {@code TypeToken}. The only reason why
 * {@code TypeToken} is not used directly is because Guava is not exposed in the driver's public API
 * (it's used internally, but shaded).
 */
@Immutable
public class GenericType<T> {

  public static final GenericType<Boolean> BOOLEAN = of(Boolean.class);
  public static final GenericType<Byte> BYTE = of(Byte.class);
  public static final GenericType<Double> DOUBLE = of(Double.class);
  public static final GenericType<Float> FLOAT = of(Float.class);
  public static final GenericType<Integer> INTEGER = of(Integer.class);
  public static final GenericType<Long> LONG = of(Long.class);
  public static final GenericType<Short> SHORT = of(Short.class);
  public static final GenericType<Instant> INSTANT = of(Instant.class);
  public static final GenericType<ZonedDateTime> ZONED_DATE_TIME = of(ZonedDateTime.class);
  public static final GenericType<LocalDate> LOCAL_DATE = of(LocalDate.class);
  public static final GenericType<LocalTime> LOCAL_TIME = of(LocalTime.class);
  public static final GenericType<LocalDateTime> LOCAL_DATE_TIME = of(LocalDateTime.class);
  public static final GenericType<ByteBuffer> BYTE_BUFFER = of(ByteBuffer.class);
  public static final GenericType<String> STRING = of(String.class);
  public static final GenericType<BigInteger> BIG_INTEGER = of(BigInteger.class);
  public static final GenericType<BigDecimal> BIG_DECIMAL = of(BigDecimal.class);
  public static final GenericType<UUID> UUID = of(UUID.class);
  public static final GenericType<InetAddress> INET_ADDRESS = of(InetAddress.class);
  public static final GenericType<CqlDuration> CQL_DURATION = of(CqlDuration.class);
  public static final GenericType<TupleValue> TUPLE_VALUE = of(TupleValue.class);
  public static final GenericType<UdtValue> UDT_VALUE = of(UdtValue.class);
  public static final GenericType<Duration> DURATION = of(Duration.class);

  @NonNull
  public static <T> GenericType<T> of(@NonNull Class<T> type) {
    return new SimpleGenericType<>(type);
  }

  @NonNull
  public static GenericType<?> of(@NonNull java.lang.reflect.Type type) {
    return new GenericType<>(TypeToken.of(type));
  }

  @NonNull
  public static <T> GenericType<List<T>> listOf(@NonNull Class<T> elementType) {
    TypeToken<List<T>> token =
        new TypeToken<List<T>>() {}.where(new TypeParameter<T>() {}, TypeToken.of(elementType));
    return new GenericType<>(token);
  }

  @NonNull
  public static <T> GenericType<List<T>> listOf(@NonNull GenericType<T> elementType) {
    TypeToken<List<T>> token =
        new TypeToken<List<T>>() {}.where(new TypeParameter<T>() {}, elementType.token);
    return new GenericType<>(token);
  }

  @NonNull
  public static <T> GenericType<Set<T>> setOf(@NonNull Class<T> elementType) {
    TypeToken<Set<T>> token =
        new TypeToken<Set<T>>() {}.where(new TypeParameter<T>() {}, TypeToken.of(elementType));
    return new GenericType<>(token);
  }

  @NonNull
  public static <T> GenericType<Set<T>> setOf(@NonNull GenericType<T> elementType) {
    TypeToken<Set<T>> token =
        new TypeToken<Set<T>>() {}.where(new TypeParameter<T>() {}, elementType.token);
    return new GenericType<>(token);
  }

  @NonNull
  public static <T extends Number> GenericType<CqlVector<T>> vectorOf(
      @NonNull Class<T> elementType) {
    TypeToken<CqlVector<T>> token =
        new TypeToken<CqlVector<T>>() {}.where(
            new TypeParameter<T>() {}, TypeToken.of(elementType));
    return new GenericType<>(token);
  }

  @NonNull
  public static <T extends Number> GenericType<CqlVector<T>> vectorOf(
      @NonNull GenericType<T> elementType) {
    TypeToken<CqlVector<T>> token =
        new TypeToken<CqlVector<T>>() {}.where(new TypeParameter<T>() {}, elementType.token);
    return new GenericType<>(token);
  }

  @NonNull
  public static <K, V> GenericType<Map<K, V>> mapOf(
      @NonNull Class<K> keyType, @NonNull Class<V> valueType) {
    TypeToken<Map<K, V>> token =
        new TypeToken<Map<K, V>>() {}.where(new TypeParameter<K>() {}, TypeToken.of(keyType))
            .where(new TypeParameter<V>() {}, TypeToken.of(valueType));
    return new GenericType<>(token);
  }

  @NonNull
  public static <K, V> GenericType<Map<K, V>> mapOf(
      @NonNull GenericType<K> keyType, @NonNull GenericType<V> valueType) {
    TypeToken<Map<K, V>> token =
        new TypeToken<Map<K, V>>() {}.where(new TypeParameter<K>() {}, keyType.token)
            .where(new TypeParameter<V>() {}, valueType.token);
    return new GenericType<>(token);
  }

  @NonNull
  public static <T> GenericType<T[]> arrayOf(@NonNull Class<T> componentType) {
    TypeToken<T[]> token =
        new TypeToken<T[]>() {}.where(new TypeParameter<T>() {}, TypeToken.of(componentType));
    return new GenericType<>(token);
  }

  @NonNull
  public static <T> GenericType<T[]> arrayOf(@NonNull GenericType<T> componentType) {
    TypeToken<T[]> token =
        new TypeToken<T[]>() {}.where(new TypeParameter<T>() {}, componentType.token);
    return new GenericType<>(token);
  }

  @NonNull
  public static <T> GenericType<Optional<T>> optionalOf(@NonNull Class<T> componentType) {
    TypeToken<Optional<T>> token =
        new TypeToken<Optional<T>>() {}.where(
            new TypeParameter<T>() {}, TypeToken.of(componentType));
    return new GenericType<>(token);
  }

  @NonNull
  public static <T> GenericType<Optional<T>> optionalOf(@NonNull GenericType<T> componentType) {
    TypeToken<Optional<T>> token =
        new TypeToken<Optional<T>>() {}.where(new TypeParameter<T>() {}, componentType.token);
    return new GenericType<>(token);
  }

  private final TypeToken<T> token;

  private GenericType(TypeToken<T> token) {
    this.token = token;
  }

  protected GenericType() {
    this.token = new TypeToken<T>(getClass()) {};
  }

  /**
   * Returns true if this type is a supertype of the given {@code type}. "Supertype" is defined
   * according to <a
   * href="http://docs.oracle.com/javase/specs/jls/se8/html/jls-4.html#jls-4.5.1">the rules for type
   * arguments</a> introduced with Java generics.
   */
  public final boolean isSupertypeOf(@NonNull GenericType<?> type) {
    return token.isSupertypeOf(type.token);
  }

  /**
   * Returns true if this type is a subtype of the given {@code type}. "Subtype" is defined
   * according to <a
   * href="http://docs.oracle.com/javase/specs/jls/se8/html/jls-4.html#jls-4.5.1">the rules for type
   * arguments</a> introduced with Java generics.
   */
  public final boolean isSubtypeOf(@NonNull GenericType<?> type) {
    return token.isSubtypeOf(type.token);
  }

  /**
   * Returns true if this type is known to be an array type, such as {@code int[]}, {@code T[]},
   * {@code <? extends Map<String, Integer>[]>} etc.
   */
  public final boolean isArray() {
    return token.isArray();
  }

  /** Returns true if this type is one of the nine primitive types (including {@code void}). */
  public final boolean isPrimitive() {
    return token.isPrimitive();
  }

  /**
   * Returns the corresponding wrapper type if this is a primitive type; otherwise returns {@code
   * this} itself. Idempotent.
   */
  @NonNull
  public final GenericType<T> wrap() {
    if (isPrimitive()) {
      return new GenericType<>(token.wrap());
    }
    return this;
  }

  /**
   * Returns the corresponding primitive type if this is a wrapper type; otherwise returns {@code
   * this} itself. Idempotent.
   */
  @NonNull
  public final GenericType<T> unwrap() {
    if (Primitives.allWrapperTypes().contains(token.getRawType())) {
      return new GenericType<>(token.unwrap());
    }
    return this;
  }

  /**
   * Substitutes a free type variable with an actual type. See {@link GenericType this class's
   * javadoc} for an example.
   */
  @NonNull
  public final <X> GenericType<T> where(
      @NonNull GenericTypeParameter<X> freeVariable, @NonNull GenericType<X> actualType) {
    TypeResolver resolver =
        new TypeResolver().where(freeVariable.getTypeVariable(), actualType.__getToken().getType());
    Type resolvedType = resolver.resolveType(this.token.getType());
    @SuppressWarnings("unchecked")
    TypeToken<T> resolvedToken = (TypeToken<T>) TypeToken.of(resolvedType);
    return new GenericType<>(resolvedToken);
  }

  /**
   * Substitutes a free type variable with an actual type. See {@link GenericType this class's
   * javadoc} for an example.
   */
  @NonNull
  public final <X> GenericType<T> where(
      @NonNull GenericTypeParameter<X> freeVariable, @NonNull Class<X> actualType) {
    return where(freeVariable, GenericType.of(actualType));
  }

  /**
   * Returns the array component type if this type represents an array ({@code int[]}, {@code T[]},
   * {@code <? extends Map<String, Integer>[]>} etc.), or else {@code null} is returned.
   */
  @Nullable
  @SuppressWarnings("unchecked")
  public final GenericType<?> getComponentType() {
    TypeToken<?> componentTypeToken = token.getComponentType();
    return (componentTypeToken == null) ? null : new GenericType(componentTypeToken);
  }

  /**
   * Returns the raw type of {@code T}. Formally speaking, if {@code T} is returned by {@link
   * java.lang.reflect.Method#getGenericReturnType}, the raw type is what's returned by {@link
   * java.lang.reflect.Method#getReturnType} of the same method object. Specifically:
   *
   * <ul>
   *   <li>If {@code T} is a {@code Class} itself, {@code T} itself is returned.
   *   <li>If {@code T} is a parameterized type, the raw type of the parameterized type is returned.
   *   <li>If {@code T} is an array type , the returned type is the corresponding array class. For
   *       example: {@code List<Integer>[] => List[]}.
   *   <li>If {@code T} is a type variable or a wildcard type, the raw type of the first upper bound
   *       is returned. For example: {@code <X extends Foo> => Foo}.
   * </ul>
   */
  @NonNull
  public Class<? super T> getRawType() {
    return token.getRawType();
  }

  /**
   * Returns the generic form of {@code superclass}. For example, if this is {@code
   * ArrayList<String>}, {@code Iterable<String>} is returned given the input {@code
   * Iterable.class}.
   */
  @SuppressWarnings("unchecked")
  @NonNull
  public final GenericType<? super T> getSupertype(@NonNull Class<? super T> superclass) {
    return new GenericType(token.getSupertype(superclass));
  }

  /**
   * Returns subtype of {@code this} with {@code subclass} as the raw class. For example, if this is
   * {@code Iterable<String>} and {@code subclass} is {@code List}, {@code List<String>} is
   * returned.
   */
  @SuppressWarnings("unchecked")
  @NonNull
  public final GenericType<? extends T> getSubtype(@NonNull Class<?> subclass) {
    return new GenericType(token.getSubtype(subclass));
  }

  /** Returns the represented type. */
  @NonNull
  public final Type getType() {
    return token.getType();
  }

  /**
   * This method is for internal use, <b>DO NOT use it from client code</b>.
   *
   * <p>It leaks a shaded type. This should be part of the internal API, but due to internal
   * implementation details it has to be exposed here.
   *
   * @leaks-private-api
   */
  @NonNull
  public TypeToken<T> __getToken() {
    return token;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof GenericType) {
      GenericType that = (GenericType) other;
      return this.token.equals(that.token);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return token.hashCode();
  }

  @Override
  public String toString() {
    return token.toString();
  }

  private static class SimpleGenericType<T> extends GenericType<T> {
    SimpleGenericType(Class<T> type) {
      super(TypeToken.of(type));
    }
  }
}
