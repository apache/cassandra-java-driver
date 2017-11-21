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
package com.datastax.oss.driver.api.core.type.reflect;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.GettableByIndex;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeResolver;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
public class GenericType<T> {

  public static final GenericType<Boolean> BOOLEAN = of(Boolean.class);
  public static final GenericType<Byte> BYTE = of(Byte.class);
  public static final GenericType<Double> DOUBLE = of(Double.class);
  public static final GenericType<Float> FLOAT = of(Float.class);
  public static final GenericType<Integer> INTEGER = of(Integer.class);
  public static final GenericType<Long> LONG = of(Long.class);
  public static final GenericType<Short> SHORT = of(Short.class);
  public static final GenericType<Instant> INSTANT = of(Instant.class);
  public static final GenericType<LocalDate> LOCAL_DATE = of(LocalDate.class);
  public static final GenericType<LocalTime> LOCAL_TIME = of(LocalTime.class);
  public static final GenericType<ByteBuffer> BYTE_BUFFER = of(ByteBuffer.class);
  public static final GenericType<String> STRING = of(String.class);
  public static final GenericType<BigInteger> BIG_INTEGER = of(BigInteger.class);
  public static final GenericType<BigDecimal> BIG_DECIMAL = of(BigDecimal.class);
  public static final GenericType<UUID> UUID = of(UUID.class);
  public static final GenericType<InetAddress> INET_ADDRESS = of(InetAddress.class);
  public static final GenericType<CqlDuration> CQL_DURATION = of(CqlDuration.class);
  public static final GenericType<TupleValue> TUPLE_VALUE = of(TupleValue.class);
  public static final GenericType<UdtValue> UDT_VALUE = of(UdtValue.class);

  public static <T> GenericType<T> of(Class<T> type) {
    return new SimpleGenericType<>(type);
  }

  public static GenericType<?> of(java.lang.reflect.Type type) {
    return new GenericType<>(TypeToken.of(type));
  }

  public static <T> GenericType<List<T>> listOf(Class<T> elementType) {
    TypeToken<List<T>> token =
        new TypeToken<List<T>>() {}.where(new TypeParameter<T>() {}, TypeToken.of(elementType));
    return new GenericType<>(token);
  }

  public static <T> GenericType<List<T>> listOf(GenericType<T> elementType) {
    TypeToken<List<T>> token =
        new TypeToken<List<T>>() {}.where(new TypeParameter<T>() {}, elementType.token);
    return new GenericType<>(token);
  }

  public static <T> GenericType<Set<T>> setOf(Class<T> elementType) {
    TypeToken<Set<T>> token =
        new TypeToken<Set<T>>() {}.where(new TypeParameter<T>() {}, TypeToken.of(elementType));
    return new GenericType<>(token);
  }

  public static <T> GenericType<Set<T>> setOf(GenericType<T> elementType) {
    TypeToken<Set<T>> token =
        new TypeToken<Set<T>>() {}.where(new TypeParameter<T>() {}, elementType.token);
    return new GenericType<>(token);
  }

  public static <K, V> GenericType<Map<K, V>> mapOf(Class<K> keyType, Class<V> valueType) {
    TypeToken<Map<K, V>> token =
        new TypeToken<Map<K, V>>() {}.where(new TypeParameter<K>() {}, TypeToken.of(keyType))
            .where(new TypeParameter<V>() {}, TypeToken.of(valueType));
    return new GenericType<>(token);
  }

  public static <K, V> GenericType<Map<K, V>> mapOf(
      GenericType<K> keyType, GenericType<V> valueType) {
    TypeToken<Map<K, V>> token =
        new TypeToken<Map<K, V>>() {}.where(new TypeParameter<K>() {}, keyType.token)
            .where(new TypeParameter<V>() {}, valueType.token);
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
   * Substitutes a free type variable with an actual type. See {@link GenericType this class's
   * javadoc} for an example.
   */
  public final <X> GenericType<T> where(
      GenericTypeParameter<X> freeVariable, GenericType<X> actualType) {
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
  public final <X> GenericType<T> where(GenericTypeParameter<X> freeVariable, Class<X> actualType) {
    return where(freeVariable, GenericType.of(actualType));
  }

  /**
   * This method is for internal use, <b>DO NOT use it from client code</b>.
   *
   * <p>It leaks a shaded type. This should be part of the internal API, but due to internal
   * implementation details it has to be exposed here.
   *
   * @leaks-private-api
   */
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
