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
package com.datastax.oss.driver.internal.core.type.codec.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.data.DefaultTupleValue;
import com.datastax.oss.driver.internal.core.data.DefaultUdtValue;
import com.datastax.oss.driver.internal.core.type.codec.CqlIntToStringCodec;
import com.datastax.oss.driver.internal.core.type.codec.IntCodec;
import com.datastax.oss.driver.internal.core.type.codec.ListCodec;
import com.datastax.oss.driver.internal.core.type.codec.registry.CachingCodecRegistryTest.TestCachingCodecRegistry.MockCache;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Period;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(DataProviderRunner.class)
public class CachingCodecRegistryTest {

  @Mock private MockCache mockCache;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  @UseDataProvider(
      value = "primitiveCodecs",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_find_primitive_codecs_for_types(TypeCodec<?> codec) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    DataType cqlType = codec.getCqlType();
    GenericType<?> javaType = codec.getJavaType();
    assertThat(registry.codecFor(cqlType, javaType)).isSameAs(codec);
    assertThat(registry.codecFor(cqlType)).isSameAs(codec);
    assertThat(javaType.__getToken().getType()).isInstanceOf(Class.class);
    Class<?> javaClass = (Class<?>) javaType.__getToken().getType();
    assertThat(registry.codecFor(cqlType, javaClass)).isSameAs(codec);
    // Primitive mappings never hit the cache
    verifyZeroInteractions(mockCache);
  }

  @Test
  @UseDataProvider(
      value = "primitiveCodecsWithValues",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_find_primitive_codecs_for_value(Object value, TypeCodec<?> codec) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    assertThat(registry.codecFor(value)).isEqualTo(codec);
    verifyZeroInteractions(mockCache);
  }

  @Test
  @UseDataProvider(
      value = "primitiveCodecsWithCqlTypesAndValues",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_find_primitive_codecs_for_cql_type_and_value(
      DataType cqlType, Object value, TypeCodec<?> codec) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    assertThat(registry.codecFor(cqlType, value)).isEqualTo(codec);
    verifyZeroInteractions(mockCache);
  }

  @Test
  public void should_find_user_codec_for_built_in_java_type() {
    // int and String are built-in types, but int <-> String is not a built-in mapping
    CqlIntToStringCodec intToStringCodec1 = new CqlIntToStringCodec();
    // register a second codec to also check that the first one is preferred
    CqlIntToStringCodec intToStringCodec2 = new CqlIntToStringCodec();
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    registry.register(intToStringCodec1, intToStringCodec2);
    verify(mockCache).lookup(DataTypes.INT, GenericType.STRING, false);

    // When the mapping is not ambiguous, the user type should be returned
    assertThat(registry.codecFor(DataTypes.INT, GenericType.STRING)).isSameAs(intToStringCodec1);
    assertThat(registry.codecFor(DataTypes.INT, String.class)).isSameAs(intToStringCodec1);
    assertThat(registry.codecFor(DataTypes.INT, "123")).isSameAs(intToStringCodec1);

    // When there is an ambiguity with a built-in codec, the built-in codec should have priority
    assertThat(registry.codecFor(DataTypes.INT)).isSameAs(TypeCodecs.INT);
    assertThat(registry.codecFor("123")).isSameAs(TypeCodecs.TEXT);

    verifyZeroInteractions(mockCache);
  }

  @Test
  public void should_find_user_codec_for_custom_java_type() {
    TextToPeriodCodec textToPeriodCodec1 = new TextToPeriodCodec();
    TextToPeriodCodec textToPeriodCodec2 = new TextToPeriodCodec();
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    registry.register(textToPeriodCodec1, textToPeriodCodec2);
    verify(mockCache).lookup(DataTypes.TEXT, GenericType.of(Period.class), false);

    assertThat(registry.codecFor(DataTypes.TEXT, GenericType.of(Period.class)))
        .isSameAs(textToPeriodCodec1);
    assertThat(registry.codecFor(DataTypes.TEXT, Period.class)).isSameAs(textToPeriodCodec1);
    assertThat(registry.codecFor(DataTypes.TEXT, Period.ofDays(1))).isSameAs(textToPeriodCodec1);
    // Now even the search by Java value only is not ambiguous
    assertThat(registry.codecFor(Period.ofDays(1))).isSameAs(textToPeriodCodec1);

    // The search by CQL type only still returns the built-in codec
    assertThat(registry.codecFor(DataTypes.TEXT)).isSameAs(TypeCodecs.TEXT);

    verifyZeroInteractions(mockCache);
  }

  @Test
  @UseDataProvider(
      value = "collectionsWithCqlAndJavaTypes",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_create_collection_codec_for_cql_and_java_types(
      DataType cqlType, GenericType<?> javaType, Object value) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = inOrder(mockCache);
    TypeCodec<?> codec = registry.codecFor(cqlType, javaType);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, javaType, false);
  }

  @Test
  @UseDataProvider(
      value = "collectionsWithCqlAndJavaTypes",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_create_collection_codec_for_cql_type(
      DataType cqlType, GenericType<?> javaType, Object value) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = inOrder(mockCache);
    TypeCodec<?> codec = registry.codecFor(cqlType);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, null, false);
  }

  @Test
  @UseDataProvider(
      value = "collectionsWithCqlAndJavaTypes",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_create_collection_codec_for_cql_type_and_java_value(
      DataType cqlType, GenericType<?> javaType, GenericType<?> javaTypeLookup, Object value) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = inOrder(mockCache);
    TypeCodec<?> codec = registry.codecFor(cqlType, value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, javaTypeLookup, true);
  }

  @Test
  @UseDataProvider(
      value = "collectionsWithCqlAndJavaTypes",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_create_collection_codec_for_java_value(
      DataType cqlType, GenericType<?> javaType, GenericType<?> javaTypeLookup, Object value) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = inOrder(mockCache);
    TypeCodec<?> codec = registry.codecFor(value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, javaTypeLookup, true);
  }

  @Test
  @UseDataProvider(
      value = "emptyCollectionsWithCqlAndJavaTypes",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_create_collection_codec_for_empty_java_value(
      DataType cqlType,
      GenericType<?> javaType,
      DataType cqlTypeLookup,
      GenericType<?> javaTypeLookup,
      Object value) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = inOrder(mockCache);
    TypeCodec<Object> codec = registry.codecFor(value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isFalse();
    assertThat(codec.accepts(javaType)).isFalse();
    assertThat(codec.accepts(value)).isTrue();

    // Note that empty collections without CQL type are a corner case, in that the registry returns
    // a codec that does not accept cqlType, nor the value's declared Java type.
    // The only requirement is that it can encode the value, which holds true:
    codec.encode(value, ProtocolVersion.DEFAULT);

    inOrder.verify(mockCache).lookup(cqlTypeLookup, javaTypeLookup, true);
  }

  @Test
  @UseDataProvider(
      value = "emptyCollectionsWithCqlAndJavaTypes",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_create_collection_codec_for_cql_type_and_empty_java_value(
      DataType cqlType, GenericType<?> javaType, Object value) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = inOrder(mockCache);
    TypeCodec<Object> codec = registry.codecFor(cqlType, value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    // Verify that the codec can encode the value
    codec.encode(value, ProtocolVersion.DEFAULT);
    inOrder.verify(mockCache).lookup(cqlType, javaType, true);
  }

  @Test
  @UseDataProvider(
      value = "collectionsWithNullElements",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_throw_for_collection_containing_null_element(Object value, String expected) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    assertThatThrownBy(() -> registry.codecFor(value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expected);
  }

  @Test
  @UseDataProvider(
      value = "tuplesWithCqlTypes",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_create_tuple_codec_for_cql_and_java_types(DataType cqlType, Object value) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = inOrder(mockCache);
    TypeCodec<TupleValue> codec = registry.codecFor(cqlType, GenericType.TUPLE_VALUE);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(GenericType.TUPLE_VALUE)).isTrue();
    assertThat(codec.accepts(TupleValue.class)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, GenericType.TUPLE_VALUE, false);
    // field codecs are only looked up when fields are accessed, so no cache hit for list<int> now
  }

  @Test
  @UseDataProvider(
      value = "tuplesWithCqlTypes",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_create_tuple_codec_for_cql_type(DataType cqlType, Object value) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = inOrder(mockCache);
    TypeCodec<TupleValue> codec = registry.codecFor(cqlType);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(GenericType.TUPLE_VALUE)).isTrue();
    assertThat(codec.accepts(TupleValue.class)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, null, false);
  }

  @Test
  @UseDataProvider(
      value = "tuplesWithCqlTypes",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_create_tuple_codec_for_cql_type_and_java_value(
      DataType cqlType, Object value) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = inOrder(mockCache);
    TypeCodec<?> codec = registry.codecFor(cqlType, value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(GenericType.TUPLE_VALUE)).isTrue();
    assertThat(codec.accepts(TupleValue.class)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, GenericType.of(DefaultTupleValue.class), true);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  @UseDataProvider(
      value = "tuplesWithCqlTypes",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_create_tuple_codec_for_java_value(DataType cqlType, Object value) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = inOrder(mockCache);
    TypeCodec<?> codec = registry.codecFor(value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(GenericType.TUPLE_VALUE)).isTrue();
    assertThat(codec.accepts(TupleValue.class)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, GenericType.of(DefaultTupleValue.class), true);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  @UseDataProvider(
      value = "udtsWithCqlTypes",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_create_udt_codec_for_cql_and_java_types(DataType cqlType, Object value) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = inOrder(mockCache);
    TypeCodec<UdtValue> codec = registry.codecFor(cqlType, GenericType.UDT_VALUE);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(GenericType.UDT_VALUE)).isTrue();
    assertThat(codec.accepts(UdtValue.class)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, GenericType.UDT_VALUE, false);
    // field codecs are only looked up when fields are accessed, so no cache hit for list<int> now

  }

  @Test
  @UseDataProvider(
      value = "udtsWithCqlTypes",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_create_udt_codec_for_cql_type(DataType cqlType, Object value) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = inOrder(mockCache);
    TypeCodec<UdtValue> codec = registry.codecFor(cqlType);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(GenericType.UDT_VALUE)).isTrue();
    assertThat(codec.accepts(UdtValue.class)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, null, false);
  }

  @Test
  @UseDataProvider(
      value = "udtsWithCqlTypes",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_create_udt_codec_for_cql_type_and_java_value(DataType cqlType, Object value) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = inOrder(mockCache);
    TypeCodec<?> codec = registry.codecFor(cqlType, value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(GenericType.UDT_VALUE)).isTrue();
    assertThat(codec.accepts(UdtValue.class)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, GenericType.of(DefaultUdtValue.class), true);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  @UseDataProvider(
      value = "udtsWithCqlTypes",
      location = CachingCodecRegistryTestDataProviders.class)
  public void should_create_udt_codec_for_java_value(DataType cqlType, Object value) {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = inOrder(mockCache);
    TypeCodec<?> codec = registry.codecFor(value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(GenericType.UDT_VALUE)).isTrue();
    assertThat(codec.accepts(UdtValue.class)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, GenericType.of(DefaultUdtValue.class), true);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void should_not_find_codec_if_java_type_unknown() {
    try {
      CodecRegistry.DEFAULT.codecFor(StringBuilder.class);
      fail("Should not have found a codec for ANY <-> StringBuilder");
    } catch (CodecNotFoundException e) {
      // expected
    }
    try {
      CodecRegistry.DEFAULT.codecFor(DataTypes.TEXT, StringBuilder.class);
      fail("Should not have found a codec for varchar <-> StringBuilder");
    } catch (CodecNotFoundException e) {
      // expected
    }
    try {
      CodecRegistry.DEFAULT.codecFor(new StringBuilder());
      fail("Should not have found a codec for ANY <-> StringBuilder");
    } catch (CodecNotFoundException e) {
      // expected
    }
  }

  @Test
  public void should_not_allow_covariance_for_lookups_by_java_type() {

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    registry.register(new ACodec());
    InOrder inOrder = inOrder(mockCache);

    // covariance not allowed

    assertThatThrownBy(() -> registry.codecFor(B.class))
        .isInstanceOf(CodecNotFoundException.class)
        .hasMessage("Codec not found for requested operation: [null <-> %s]", B.class.getName());
    // because of invariance, the custom A codec doesn't match so we try the cache
    inOrder.verify(mockCache).lookup(null, GenericType.of(B.class), false);
    inOrder.verifyNoMoreInteractions();

    assertThatThrownBy(() -> registry.codecFor(GenericType.listOf(B.class)))
        .isInstanceOf(CodecNotFoundException.class);
    inOrder.verify(mockCache).lookup(null, GenericType.listOf(B.class), false);
    inOrder.verify(mockCache).lookup(null, GenericType.of(B.class), false);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void should_allow_covariance_for_lookups_by_cql_type_and_value() {

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    registry.register(new ACodec());
    InOrder inOrder = inOrder(mockCache);
    inOrder.verify(mockCache).lookup(DataTypes.INT, GenericType.of(A.class), false);

    // covariance allowed

    assertThat(registry.codecFor(DataTypes.INT, new B())).isInstanceOf(ACodec.class);
    // no cache hit since we find the custom codec directly
    inOrder.verifyNoMoreInteractions();

    // note: in Java, type parameters are always invariant, so List<B> is not a subtype of List<A>;
    // but in practice, a codec for List<A> is capable of encoding a List<B>, so we allow it (even
    // if in driver 3.x that was forbidden).
    List<B> list = Lists.newArrayList(new B());
    ListType cqlType = DataTypes.listOf(DataTypes.INT);
    TypeCodec<List<B>> actual = registry.codecFor(cqlType, list);
    assertThat(actual).isInstanceOf(ListCodec.class);
    assertThat(actual.getJavaType()).isEqualTo(GenericType.listOf(A.class));
    assertThat(actual.accepts(list)).isTrue();
    // accepts(GenericType) remains invariant, so it returns false for List<B>
    assertThat(actual.accepts(GenericType.listOf(B.class))).isFalse();
    inOrder.verify(mockCache).lookup(cqlType, GenericType.listOf(B.class), true);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void should_allow_covariance_for_lookups_by_value() {

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    registry.register(new ACodec());
    InOrder inOrder = inOrder(mockCache);
    inOrder.verify(mockCache).lookup(DataTypes.INT, GenericType.of(A.class), false);

    // covariance allowed

    assertThat(registry.codecFor(new B())).isInstanceOf(ACodec.class);
    // no cache hit since we find the custom codec directly
    inOrder.verifyNoMoreInteractions();

    // note: in Java, type parameters are always invariant, so List<B> is not a subtype of List<A>;
    // but in practice, a codec for List<A> is capable of encoding a List<B>, so we allow it (even
    // if in driver 3.x that was forbidden).
    List<B> list = Lists.newArrayList(new B());
    TypeCodec<List<B>> actual = registry.codecFor(list);
    assertThat(actual).isInstanceOf(ListCodec.class);
    assertThat(actual.getJavaType()).isEqualTo(GenericType.listOf(A.class));
    assertThat(actual.accepts(list)).isTrue();
    // accepts(GenericType) remains invariant, so it returns false for List<B>
    assertThat(actual.accepts(GenericType.listOf(B.class))).isFalse();
    inOrder.verify(mockCache).lookup(null, GenericType.listOf(B.class), true);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void should_register_user_codec_at_runtime() {
    CqlIntToStringCodec intToStringCodec = new CqlIntToStringCodec();
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    registry.register(intToStringCodec);
    // register checks the cache for collisions
    verify(mockCache).lookup(DataTypes.INT, GenericType.STRING, false);

    // When the mapping is not ambiguous, the user type should be returned
    assertThat(registry.codecFor(DataTypes.INT, GenericType.STRING)).isSameAs(intToStringCodec);
    assertThat(registry.codecFor(DataTypes.INT, String.class)).isSameAs(intToStringCodec);
    assertThat(registry.codecFor(DataTypes.INT, "123")).isSameAs(intToStringCodec);

    // When there is an ambiguity with a built-in codec, the built-in codec should have priority
    assertThat(registry.codecFor(DataTypes.INT)).isSameAs(TypeCodecs.INT);
    assertThat(registry.codecFor("123")).isSameAs(TypeCodecs.TEXT);

    verifyZeroInteractions(mockCache);
  }

  @Test
  public void should_ignore_user_codec_if_collides_with_builtin_codec() {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);

    IntCodec userIntCodec = new IntCodec();
    registry.register(userIntCodec);

    assertThat(registry.codecFor(DataTypes.INT, Integer.class)).isNotSameAs(userIntCodec);
  }

  @Test
  public void should_ignore_user_codec_if_collides_with_other_user_codec() {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    CqlIntToStringCodec intToStringCodec1 = new CqlIntToStringCodec();
    CqlIntToStringCodec intToStringCodec2 = new CqlIntToStringCodec();

    registry.register(intToStringCodec1, intToStringCodec2);

    assertThat(registry.codecFor(DataTypes.INT, GenericType.STRING)).isSameAs(intToStringCodec1);
  }

  @Test
  public void should_ignore_user_codec_if_collides_with_generated_codec() {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);

    TypeCodec<List<Integer>> userListOfIntCodec = TypeCodecs.listOf(TypeCodecs.INT);
    registry.register(userListOfIntCodec);

    assertThat(
            registry.codecFor(DataTypes.listOf(DataTypes.INT), GenericType.listOf(Integer.class)))
        .isNotSameAs(userListOfIntCodec);
  }

  // Our intent is not to test Guava cache, so we don't need an actual cache here.
  // The only thing we want to check in our tests is if getCachedCodec was called.
  public static class TestCachingCodecRegistry extends CachingCodecRegistry {
    private final MockCache cache;

    TestCachingCodecRegistry(MockCache cache) {
      super("test", CodecRegistryConstants.PRIMITIVE_CODECS);
      this.cache = cache;
    }

    @Override
    protected TypeCodec<?> getCachedCodec(
        @Nullable DataType cqlType, @Nullable GenericType<?> javaType, boolean isJavaCovariant) {
      cache.lookup(cqlType, javaType, isJavaCovariant);
      return createCodec(cqlType, javaType, isJavaCovariant);
    }

    public interface MockCache {
      void lookup(
          @Nullable DataType cqlType, @Nullable GenericType<?> javaType, boolean isJavaCovariant);
    }
  }

  public static class TextToPeriodCodec implements TypeCodec<Period> {
    @NonNull
    @Override
    public GenericType<Period> getJavaType() {
      return GenericType.of(Period.class);
    }

    @NonNull
    @Override
    public DataType getCqlType() {
      return DataTypes.TEXT;
    }

    @Override
    public ByteBuffer encode(Period value, @NonNull ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException("not implemented for this test");
    }

    @Override
    public Period decode(ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException("not implemented for this test");
    }

    @NonNull
    @Override
    public String format(Period value) {
      throw new UnsupportedOperationException("not implemented for this test");
    }

    @Override
    public Period parse(String value) {
      throw new UnsupportedOperationException("not implemented for this test");
    }
  }

  private static class A {}

  private static class B extends A {}

  private static class ACodec implements TypeCodec<A> {

    @NonNull
    @Override
    public GenericType<A> getJavaType() {
      return GenericType.of(A.class);
    }

    @NonNull
    @Override
    public DataType getCqlType() {
      return DataTypes.INT;
    }

    @Override
    public ByteBuffer encode(A value, @NonNull ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException("irrelevant");
    }

    @Override
    public A decode(ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException("irrelevant");
    }

    @NonNull
    @Override
    public String format(A value) {
      throw new UnsupportedOperationException("irrelevant");
    }

    @Override
    public A parse(String value) {
      throw new UnsupportedOperationException("irrelevant");
    }
  }
}
