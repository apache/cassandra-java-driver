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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.internal.core.type.codec.CqlIntToStringCodec;
import com.datastax.oss.driver.internal.core.type.codec.ListCodec;
import com.datastax.oss.driver.internal.core.type.codec.registry.CachingCodecRegistryTest.TestCachingCodecRegistry.MockCache;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class CachingCodecRegistryTest {

  @Mock private MockCache mockCache;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void should_find_primitive_codecs_for_types() {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    checkPrimitiveMappings(registry, TypeCodecs.BOOLEAN);
    checkPrimitiveMappings(registry, TypeCodecs.TINYINT);
    checkPrimitiveMappings(registry, TypeCodecs.DOUBLE);
    checkPrimitiveMappings(registry, TypeCodecs.COUNTER);
    checkPrimitiveMappings(registry, TypeCodecs.FLOAT);
    checkPrimitiveMappings(registry, TypeCodecs.INT);
    checkPrimitiveMappings(registry, TypeCodecs.BIGINT);
    checkPrimitiveMappings(registry, TypeCodecs.SMALLINT);
    checkPrimitiveMappings(registry, TypeCodecs.TIMESTAMP);
    checkPrimitiveMappings(registry, TypeCodecs.DATE);
    checkPrimitiveMappings(registry, TypeCodecs.TIME);
    checkPrimitiveMappings(registry, TypeCodecs.BLOB);
    checkPrimitiveMappings(registry, TypeCodecs.TEXT);
    checkPrimitiveMappings(registry, TypeCodecs.ASCII);
    checkPrimitiveMappings(registry, TypeCodecs.VARINT);
    checkPrimitiveMappings(registry, TypeCodecs.DECIMAL);
    checkPrimitiveMappings(registry, TypeCodecs.UUID);
    checkPrimitiveMappings(registry, TypeCodecs.TIMEUUID);
    checkPrimitiveMappings(registry, TypeCodecs.INET);
    checkPrimitiveMappings(registry, TypeCodecs.DURATION);
    // Primitive mappings never hit the cache
    Mockito.verifyZeroInteractions(mockCache);
  }

  private void checkPrimitiveMappings(TestCachingCodecRegistry registry, TypeCodec<?> codec) {
    DataType cqlType = codec.getCqlType();
    GenericType<?> javaType = codec.getJavaType();

    assertThat(registry.codecFor(cqlType, javaType)).isSameAs(codec);
    assertThat(registry.codecFor(cqlType)).isSameAs(codec);

    assertThat(javaType.__getToken().getType()).isInstanceOf(Class.class);
    Class<?> javaClass = (Class<?>) javaType.__getToken().getType();
    assertThat(registry.codecFor(cqlType, javaClass)).isSameAs(codec);
  }

  @Test
  public void should_find_primitive_codecs_for_value() throws Exception {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    assertThat(registry.codecFor(true)).isEqualTo(TypeCodecs.BOOLEAN);
    assertThat(registry.codecFor((byte) 0)).isEqualTo(TypeCodecs.TINYINT);
    assertThat(registry.codecFor(0.0)).isEqualTo(TypeCodecs.DOUBLE);
    assertThat(registry.codecFor(0.0f)).isEqualTo(TypeCodecs.FLOAT);
    assertThat(registry.codecFor(0)).isEqualTo(TypeCodecs.INT);
    assertThat(registry.codecFor(0L)).isEqualTo(TypeCodecs.BIGINT);
    assertThat(registry.codecFor((short) 0)).isEqualTo(TypeCodecs.SMALLINT);
    assertThat(registry.codecFor(Instant.EPOCH)).isEqualTo(TypeCodecs.TIMESTAMP);
    assertThat(registry.codecFor(LocalDate.MIN)).isEqualTo(TypeCodecs.DATE);
    assertThat(registry.codecFor(LocalTime.MIDNIGHT)).isEqualTo(TypeCodecs.TIME);
    assertThat(registry.codecFor(ByteBuffer.allocate(0))).isEqualTo(TypeCodecs.BLOB);
    assertThat(registry.codecFor("")).isEqualTo(TypeCodecs.TEXT);
    assertThat(registry.codecFor(BigInteger.ONE)).isEqualTo(TypeCodecs.VARINT);
    assertThat(registry.codecFor(BigDecimal.ONE)).isEqualTo(TypeCodecs.DECIMAL);
    assertThat(registry.codecFor(new UUID(2L, 1L))).isEqualTo(TypeCodecs.UUID);
    assertThat(registry.codecFor(InetAddress.getByName("127.0.0.1"))).isEqualTo(TypeCodecs.INET);
    assertThat(registry.codecFor(CqlDuration.newInstance(1, 2, 3))).isEqualTo(TypeCodecs.DURATION);
    Mockito.verifyZeroInteractions(mockCache);
  }

  @Test
  public void should_find_primitive_codecs_for_cql_type_and_value() throws Exception {
    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    assertThat(registry.codecFor(DataTypes.BOOLEAN, true)).isEqualTo(TypeCodecs.BOOLEAN);
    assertThat(registry.codecFor(DataTypes.TINYINT, (byte) 0)).isEqualTo(TypeCodecs.TINYINT);
    assertThat(registry.codecFor(DataTypes.DOUBLE, 0.0)).isEqualTo(TypeCodecs.DOUBLE);
    assertThat(registry.codecFor(DataTypes.FLOAT, 0.0f)).isEqualTo(TypeCodecs.FLOAT);
    assertThat(registry.codecFor(DataTypes.INT, 0)).isEqualTo(TypeCodecs.INT);
    assertThat(registry.codecFor(DataTypes.BIGINT, 0L)).isEqualTo(TypeCodecs.BIGINT);
    assertThat(registry.codecFor(DataTypes.SMALLINT, (short) 0)).isEqualTo(TypeCodecs.SMALLINT);
    assertThat(registry.codecFor(DataTypes.TIMESTAMP, Instant.EPOCH))
        .isEqualTo(TypeCodecs.TIMESTAMP);
    assertThat(registry.codecFor(DataTypes.DATE, LocalDate.MIN)).isEqualTo(TypeCodecs.DATE);
    assertThat(registry.codecFor(DataTypes.TIME, LocalTime.MIDNIGHT)).isEqualTo(TypeCodecs.TIME);
    assertThat(registry.codecFor(DataTypes.BLOB, ByteBuffer.allocate(0)))
        .isEqualTo(TypeCodecs.BLOB);
    assertThat(registry.codecFor(DataTypes.TEXT, "")).isEqualTo(TypeCodecs.TEXT);
    assertThat(registry.codecFor(DataTypes.VARINT, BigInteger.ONE)).isEqualTo(TypeCodecs.VARINT);
    assertThat(registry.codecFor(DataTypes.DECIMAL, BigDecimal.ONE)).isEqualTo(TypeCodecs.DECIMAL);
    assertThat(registry.codecFor(DataTypes.UUID, new UUID(2L, 1L))).isEqualTo(TypeCodecs.UUID);
    assertThat(registry.codecFor(DataTypes.INET, InetAddress.getByName("127.0.0.1")))
        .isEqualTo(TypeCodecs.INET);
    assertThat(registry.codecFor(DataTypes.DURATION, CqlDuration.newInstance(1, 2, 3)))
        .isEqualTo(TypeCodecs.DURATION);
    Mockito.verifyZeroInteractions(mockCache);
  }

  @Test
  public void should_find_user_codec_for_built_in_java_type() {
    // int and String are built-in types, but int <-> String is not a built-in mapping
    CqlIntToStringCodec intToStringCodec1 = new CqlIntToStringCodec();
    // register a second codec to also check that the first one is preferred
    CqlIntToStringCodec intToStringCodec2 = new CqlIntToStringCodec();
    TestCachingCodecRegistry registry =
        new TestCachingCodecRegistry(mockCache, intToStringCodec1, intToStringCodec2);

    // When the mapping is not ambiguous, the user type should be returned
    assertThat(registry.codecFor(DataTypes.INT, GenericType.STRING)).isSameAs(intToStringCodec1);
    assertThat(registry.codecFor(DataTypes.INT, String.class)).isSameAs(intToStringCodec1);
    assertThat(registry.codecFor(DataTypes.INT, "")).isSameAs(intToStringCodec1);

    // When there is an ambiguity with a built-in codec, the built-in codec should have priority
    assertThat(registry.codecFor(DataTypes.INT)).isSameAs(TypeCodecs.INT);
    assertThat(registry.codecFor("")).isSameAs(TypeCodecs.TEXT);

    Mockito.verifyZeroInteractions(mockCache);
  }

  @Test
  public void should_find_user_codec_for_custom_java_type() {
    TextToPeriodCodec textToPeriodCodec1 = new TextToPeriodCodec();
    TextToPeriodCodec textToPeriodCodec2 = new TextToPeriodCodec();
    TestCachingCodecRegistry registry =
        new TestCachingCodecRegistry(mockCache, textToPeriodCodec1, textToPeriodCodec2);

    assertThat(registry.codecFor(DataTypes.TEXT, GenericType.of(Period.class)))
        .isSameAs(textToPeriodCodec1);
    assertThat(registry.codecFor(DataTypes.TEXT, Period.class)).isSameAs(textToPeriodCodec1);
    assertThat(registry.codecFor(DataTypes.TEXT, Period.ofDays(1))).isSameAs(textToPeriodCodec1);
    // Now even the search by Java value only is not ambiguous
    assertThat(registry.codecFor(Period.ofDays(1))).isSameAs(textToPeriodCodec1);

    // The search by CQL type only still returns the built-in codec
    assertThat(registry.codecFor(DataTypes.TEXT)).isSameAs(TypeCodecs.TEXT);

    Mockito.verifyZeroInteractions(mockCache);
  }

  @Test
  public void should_create_list_codec_for_cql_and_java_types() {
    ListType cqlType = DataTypes.listOf(DataTypes.listOf(DataTypes.INT));
    GenericType<List<List<Integer>>> javaType = new GenericType<List<List<Integer>>>() {};
    List<List<Integer>> value = ImmutableList.of(ImmutableList.of(1));

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<List<List<Integer>>> codec = registry.codecFor(cqlType, javaType);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    // Cache lookup for the codec, and recursively for its subcodec
    inOrder.verify(mockCache).lookup(cqlType, javaType, false);
    inOrder
        .verify(mockCache)
        .lookup(DataTypes.listOf(DataTypes.INT), GenericType.listOf(GenericType.INTEGER), false);
  }

  @Test
  public void should_create_list_codec_for_cql_type() {
    ListType cqlType = DataTypes.listOf(DataTypes.listOf(DataTypes.INT));
    GenericType<List<List<Integer>>> javaType = new GenericType<List<List<Integer>>>() {};
    List<List<Integer>> value = ImmutableList.of(ImmutableList.of(1));

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<List<List<Integer>>> codec = registry.codecFor(cqlType);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, null, false);
    inOrder.verify(mockCache).lookup(DataTypes.listOf(DataTypes.INT), null, false);
  }

  @Test
  public void should_create_list_codec_for_cql_type_and_java_value() {
    ListType cqlType = DataTypes.listOf(DataTypes.listOf(DataTypes.INT));
    GenericType<List<List<Integer>>> javaType = new GenericType<List<List<Integer>>>() {};
    List<List<Integer>> value = ImmutableList.of(ImmutableList.of(1));

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<List<List<Integer>>> codec = registry.codecFor(cqlType, value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, javaType, true);
    inOrder
        .verify(mockCache)
        .lookup(DataTypes.listOf(DataTypes.INT), GenericType.listOf(GenericType.INTEGER), true);
  }

  @Test
  public void should_create_list_codec_for_java_value() {
    ListType cqlType = DataTypes.listOf(DataTypes.listOf(DataTypes.INT));
    GenericType<List<List<Integer>>> javaType = new GenericType<List<List<Integer>>>() {};
    List<List<Integer>> value = ImmutableList.of(ImmutableList.of(1));

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<List<List<Integer>>> codec = registry.codecFor(value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(null, javaType, true);
    inOrder.verify(mockCache).lookup(null, GenericType.listOf(GenericType.INTEGER), true);
  }

  @Test
  public void should_create_list_codec_for_java_value_when_first_element_is_a_subtype()
      throws UnknownHostException {
    ListType cqlType = DataTypes.listOf(DataTypes.INET);
    GenericType<List<InetAddress>> javaType = new GenericType<List<InetAddress>>() {};
    InetAddress address = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    // Because the actual implementation is a subclass, there is no exact match with the codec's
    // declared type
    assertThat(address).isInstanceOf(Inet4Address.class);
    List<InetAddress> value = ImmutableList.of(address);

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<List<InetAddress>> codec = registry.codecFor(value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();

    inOrder.verify(mockCache).lookup(null, GenericType.listOf(Inet4Address.class), true);
  }

  @Test
  public void should_create_set_codec_for_cql_and_java_types() {
    SetType cqlType = DataTypes.setOf(DataTypes.setOf(DataTypes.INT));
    GenericType<Set<Set<Integer>>> javaType = new GenericType<Set<Set<Integer>>>() {};
    Set<Set<Integer>> value = ImmutableSet.of(ImmutableSet.of(1));

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<Set<Set<Integer>>> codec = registry.codecFor(cqlType, javaType);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    // Cache lookup for the codec, and recursively for its subcodec
    inOrder.verify(mockCache).lookup(cqlType, javaType, false);
    inOrder
        .verify(mockCache)
        .lookup(DataTypes.setOf(DataTypes.INT), GenericType.setOf(GenericType.INTEGER), false);
  }

  @Test
  public void should_create_set_codec_for_cql_type() {
    SetType cqlType = DataTypes.setOf(DataTypes.setOf(DataTypes.INT));
    GenericType<Set<Set<Integer>>> javaType = new GenericType<Set<Set<Integer>>>() {};
    Set<Set<Integer>> value = ImmutableSet.of(ImmutableSet.of(1));

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<Set<Set<Integer>>> codec = registry.codecFor(cqlType);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, null, false);
    inOrder.verify(mockCache).lookup(DataTypes.setOf(DataTypes.INT), null, false);
  }

  @Test
  public void should_create_set_codec_for_cql_type_and_java_value() {
    SetType cqlType = DataTypes.setOf(DataTypes.setOf(DataTypes.INT));
    GenericType<Set<Set<Integer>>> javaType = new GenericType<Set<Set<Integer>>>() {};
    Set<Set<Integer>> value = ImmutableSet.of(ImmutableSet.of(1));

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<Set<Set<Integer>>> codec = registry.codecFor(cqlType, value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, javaType, true);
    inOrder
        .verify(mockCache)
        .lookup(DataTypes.setOf(DataTypes.INT), GenericType.setOf(GenericType.INTEGER), true);
  }

  @Test
  public void should_create_set_codec_for_java_value() {
    SetType cqlType = DataTypes.setOf(DataTypes.setOf(DataTypes.INT));
    GenericType<Set<Set<Integer>>> javaType = new GenericType<Set<Set<Integer>>>() {};
    Set<Set<Integer>> value = ImmutableSet.of(ImmutableSet.of(1));

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<Set<Set<Integer>>> codec = registry.codecFor(value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(null, javaType, true);
    inOrder.verify(mockCache).lookup(null, GenericType.setOf(GenericType.INTEGER), true);
  }

  @Test
  public void should_create_set_codec_for_java_value_when_first_element_is_a_subtype()
      throws UnknownHostException {
    SetType cqlType = DataTypes.setOf(DataTypes.INET);
    GenericType<Set<InetAddress>> javaType = new GenericType<Set<InetAddress>>() {};
    InetAddress address = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    // Because the actual implementation is a subclass, there is no exact match with the codec's
    // declared type
    assertThat(address).isInstanceOf(Inet4Address.class);
    Set<InetAddress> value = ImmutableSet.of(address);

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<Set<InetAddress>> codec = registry.codecFor(value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();

    inOrder.verify(mockCache).lookup(null, GenericType.setOf(Inet4Address.class), true);
  }

  @Test
  public void should_create_map_codec_for_cql_and_java_types() {
    MapType cqlType = DataTypes.mapOf(DataTypes.INT, DataTypes.mapOf(DataTypes.INT, DataTypes.INT));
    GenericType<Map<Integer, Map<Integer, Integer>>> javaType =
        new GenericType<Map<Integer, Map<Integer, Integer>>>() {};
    Map<Integer, Map<Integer, Integer>> value = ImmutableMap.of(1, ImmutableMap.of(1, 1));

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<Map<Integer, Map<Integer, Integer>>> codec = registry.codecFor(cqlType, javaType);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    // Cache lookup for the codec, and recursively for its subcodec
    inOrder.verify(mockCache).lookup(cqlType, javaType, false);
    inOrder
        .verify(mockCache)
        .lookup(
            DataTypes.mapOf(DataTypes.INT, DataTypes.INT),
            GenericType.mapOf(GenericType.INTEGER, GenericType.INTEGER),
            false);
  }

  @Test
  public void should_create_map_codec_for_cql_type() {
    MapType cqlType = DataTypes.mapOf(DataTypes.INT, DataTypes.mapOf(DataTypes.INT, DataTypes.INT));
    GenericType<Map<Integer, Map<Integer, Integer>>> javaType =
        new GenericType<Map<Integer, Map<Integer, Integer>>>() {};
    Map<Integer, Map<Integer, Integer>> value = ImmutableMap.of(1, ImmutableMap.of(1, 1));

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<Map<Integer, Map<Integer, Integer>>> codec = registry.codecFor(cqlType);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, null, false);
    inOrder.verify(mockCache).lookup(DataTypes.mapOf(DataTypes.INT, DataTypes.INT), null, false);
  }

  @Test
  public void should_create_map_codec_for_java_type() {
    MapType cqlType = DataTypes.mapOf(DataTypes.INT, DataTypes.mapOf(DataTypes.INT, DataTypes.INT));
    GenericType<Map<Integer, Map<Integer, Integer>>> javaType =
        new GenericType<Map<Integer, Map<Integer, Integer>>>() {};
    Map<Integer, Map<Integer, Integer>> value = ImmutableMap.of(1, ImmutableMap.of(1, 1));

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<Map<Integer, Map<Integer, Integer>>> codec = registry.codecFor(javaType);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(null, javaType, false);
    inOrder.verify(mockCache).lookup(null, new GenericType<Map<Integer, Integer>>() {}, false);
  }

  @Test
  public void should_create_map_codec_for_cql_type_and_java_value() {
    MapType cqlType = DataTypes.mapOf(DataTypes.INT, DataTypes.mapOf(DataTypes.INT, DataTypes.INT));
    GenericType<Map<Integer, Map<Integer, Integer>>> javaType =
        new GenericType<Map<Integer, Map<Integer, Integer>>>() {};
    Map<Integer, Map<Integer, Integer>> value = ImmutableMap.of(1, ImmutableMap.of(1, 1));

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<Map<Integer, Map<Integer, Integer>>> codec = registry.codecFor(cqlType, value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, javaType, true);
    inOrder
        .verify(mockCache)
        .lookup(
            DataTypes.mapOf(DataTypes.INT, DataTypes.INT),
            GenericType.mapOf(GenericType.INTEGER, GenericType.INTEGER),
            true);
  }

  @Test
  public void should_create_map_codec_for_java_value() {
    MapType cqlType = DataTypes.mapOf(DataTypes.INT, DataTypes.mapOf(DataTypes.INT, DataTypes.INT));
    GenericType<Map<Integer, Map<Integer, Integer>>> javaType =
        new GenericType<Map<Integer, Map<Integer, Integer>>>() {};
    Map<Integer, Map<Integer, Integer>> value = ImmutableMap.of(1, ImmutableMap.of(1, 1));

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<Map<Integer, Map<Integer, Integer>>> codec = registry.codecFor(value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(null, javaType, true);
    inOrder
        .verify(mockCache)
        .lookup(null, GenericType.mapOf(GenericType.INTEGER, GenericType.INTEGER), true);
  }

  @Test
  public void should_create_map_codec_for_java_value_when_first_element_is_a_subtype()
      throws UnknownHostException {
    MapType cqlType = DataTypes.mapOf(DataTypes.INET, DataTypes.INET);
    GenericType<Map<InetAddress, InetAddress>> javaType =
        new GenericType<Map<InetAddress, InetAddress>>() {};
    InetAddress address = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    // Because the actual implementation is a subclass, there is no exact match with the codec's
    // declared type
    assertThat(address).isInstanceOf(Inet4Address.class);
    Map<InetAddress, InetAddress> value = ImmutableMap.of(address, address);

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<Map<InetAddress, InetAddress>> codec = registry.codecFor(value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(javaType)).isTrue();
    assertThat(codec.accepts(value)).isTrue();

    inOrder
        .verify(mockCache)
        .lookup(null, GenericType.mapOf(Inet4Address.class, Inet4Address.class), true);
  }

  @Test
  public void should_create_tuple_codec_for_cql_and_java_types() {
    TupleType cqlType = DataTypes.tupleOf(DataTypes.INT, DataTypes.listOf(DataTypes.TEXT));
    TupleValue value = cqlType.newValue();

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

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
  public void should_create_tuple_codec_for_cql_type() {
    TupleType cqlType = DataTypes.tupleOf(DataTypes.INT, DataTypes.listOf(DataTypes.TEXT));
    TupleValue value = cqlType.newValue();

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<TupleValue> codec = registry.codecFor(cqlType);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(GenericType.TUPLE_VALUE)).isTrue();
    assertThat(codec.accepts(TupleValue.class)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, null, false);
  }

  @Test
  public void should_create_tuple_codec_for_cql_type_and_java_value() {
    TupleType cqlType = DataTypes.tupleOf(DataTypes.INT, DataTypes.listOf(DataTypes.TEXT));
    TupleValue value = cqlType.newValue();

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<TupleValue> codec = registry.codecFor(cqlType, value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(GenericType.TUPLE_VALUE)).isTrue();
    assertThat(codec.accepts(TupleValue.class)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, GenericType.TUPLE_VALUE, false);

    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void should_create_tuple_codec_for_java_value() {
    TupleType cqlType = DataTypes.tupleOf(DataTypes.INT, DataTypes.listOf(DataTypes.TEXT));
    TupleValue value = cqlType.newValue();

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<TupleValue> codec = registry.codecFor(value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(GenericType.TUPLE_VALUE)).isTrue();
    assertThat(codec.accepts(TupleValue.class)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    // UDTs know their CQL type, so the actual lookup is by CQL + Java type, and therefore not
    // covariant.
    inOrder.verify(mockCache).lookup(cqlType, GenericType.TUPLE_VALUE, false);

    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void should_create_udt_codec_for_cql_and_java_types() {
    UserDefinedType cqlType =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("field1"), DataTypes.INT)
            .withField(CqlIdentifier.fromInternal("field2"), DataTypes.listOf(DataTypes.TEXT))
            .build();
    UdtValue value = cqlType.newValue();

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

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
  public void should_create_udt_codec_for_cql_type() {
    UserDefinedType cqlType =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("field1"), DataTypes.INT)
            .withField(CqlIdentifier.fromInternal("field2"), DataTypes.listOf(DataTypes.TEXT))
            .build();
    UdtValue value = cqlType.newValue();

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<UdtValue> codec = registry.codecFor(cqlType);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(GenericType.UDT_VALUE)).isTrue();
    assertThat(codec.accepts(UdtValue.class)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, null, false);
  }

  @Test
  public void should_create_udt_codec_for_cql_type_and_java_value() {
    UserDefinedType cqlType =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("field1"), DataTypes.INT)
            .withField(CqlIdentifier.fromInternal("field2"), DataTypes.listOf(DataTypes.TEXT))
            .build();
    UdtValue value = cqlType.newValue();

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<UdtValue> codec = registry.codecFor(cqlType, value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(GenericType.UDT_VALUE)).isTrue();
    assertThat(codec.accepts(UdtValue.class)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    inOrder.verify(mockCache).lookup(cqlType, GenericType.UDT_VALUE, false);

    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void should_create_udt_codec_for_java_value() {
    UserDefinedType cqlType =
        new UserDefinedTypeBuilder(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"))
            .withField(CqlIdentifier.fromInternal("field1"), DataTypes.INT)
            .withField(CqlIdentifier.fromInternal("field2"), DataTypes.listOf(DataTypes.TEXT))
            .build();
    UdtValue value = cqlType.newValue();

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache);
    InOrder inOrder = Mockito.inOrder(mockCache);

    TypeCodec<UdtValue> codec = registry.codecFor(value);
    assertThat(codec).isNotNull();
    assertThat(codec.accepts(cqlType)).isTrue();
    assertThat(codec.accepts(GenericType.UDT_VALUE)).isTrue();
    assertThat(codec.accepts(UdtValue.class)).isTrue();
    assertThat(codec.accepts(value)).isTrue();
    // UDTs know their CQL type, so the actual lookup is by CQL + Java type, and therefore not
    // covariant.
    inOrder.verify(mockCache).lookup(cqlType, GenericType.UDT_VALUE, false);

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

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache, new ACodec());
    InOrder inOrder = Mockito.inOrder(mockCache);

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

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache, new ACodec());
    InOrder inOrder = Mockito.inOrder(mockCache);

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

    TestCachingCodecRegistry registry = new TestCachingCodecRegistry(mockCache, new ACodec());
    InOrder inOrder = Mockito.inOrder(mockCache);

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

  // Our intent is not to test Guava cache, so we don't need an actual cache here.
  // The only thing we want to check in our tests is if getCachedCodec was called.
  public static class TestCachingCodecRegistry extends CachingCodecRegistry {
    private final MockCache cache;

    public TestCachingCodecRegistry(MockCache cache, TypeCodec<?>... userCodecs) {
      super("test", CodecRegistryConstants.PRIMITIVE_CODECS, userCodecs);
      this.cache = cache;
    }

    @Override
    protected TypeCodec<?> getCachedCodec(
        DataType cqlType, GenericType<?> javaType, boolean isJavaCovariant) {
      cache.lookup(cqlType, javaType, isJavaCovariant);
      return createCodec(cqlType, javaType, isJavaCovariant);
    }

    public interface MockCache {
      void lookup(DataType cqlType, GenericType<?> javaType, boolean isJavaCovariant);
    }
  }

  public static class TextToPeriodCodec implements TypeCodec<Period> {
    @Override
    public GenericType<Period> getJavaType() {
      return GenericType.of(Period.class);
    }

    @Override
    public DataType getCqlType() {
      return DataTypes.TEXT;
    }

    @Override
    public ByteBuffer encode(Period value, ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException("not implemented for this test");
    }

    @Override
    public Period decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException("not implemented for this test");
    }

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

    @Override
    public GenericType<A> getJavaType() {
      return GenericType.of(A.class);
    }

    @Override
    public DataType getCqlType() {
      return DataTypes.INT;
    }

    @Override
    public ByteBuffer encode(A value, ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException("irrelevant");
    }

    @Override
    public A decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException("irrelevant");
    }

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
