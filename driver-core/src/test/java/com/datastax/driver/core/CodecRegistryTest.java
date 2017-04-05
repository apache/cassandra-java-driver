/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.google.common.reflect.TypeToken;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.DataType.*;
import static com.datastax.driver.core.DataType.list;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.driver.core.TypeTokens.*;
import static com.google.common.reflect.TypeToken.of;
import static java.util.Collections.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

public class CodecRegistryTest {

    @DataProvider
    public static Object[][] cql() {
        return new Object[][]{
                {DataType.blob(), TypeCodec.blob()},
                {DataType.cboolean(), TypeCodec.cboolean()},
                {DataType.smallint(), TypeCodec.smallInt()},
                {DataType.tinyint(), TypeCodec.tinyInt()},
                {DataType.cint(), TypeCodec.cint()},
                {DataType.bigint(), TypeCodec.bigint()},
                {DataType.counter(), TypeCodec.counter()},
                {DataType.cdouble(), TypeCodec.cdouble()},
                {DataType.cfloat(), TypeCodec.cfloat()},
                {DataType.varint(), TypeCodec.varint()},
                {DataType.decimal(), TypeCodec.decimal()},
                {DataType.varchar(), TypeCodec.varchar()},
                {DataType.ascii(), TypeCodec.ascii()},
                {DataType.timestamp(), TypeCodec.timestamp()},
                {DataType.date(), TypeCodec.date()},
                {DataType.time(), TypeCodec.time()},
                {DataType.uuid(), TypeCodec.uuid()},
                {DataType.timeuuid(), TypeCodec.timeUUID()},
                {DataType.inet(), TypeCodec.inet()},
                {DataType.duration(), TypeCodec.duration()}
        };
    }

    @DataProvider
    public static Object[][] cqlAndJava() {
        return new Object[][]{
                {DataType.blob(), ByteBuffer.class, TypeCodec.blob()},
                {DataType.cboolean(), Boolean.class, TypeCodec.cboolean()},
                {DataType.smallint(), Short.class, TypeCodec.smallInt()},
                {DataType.tinyint(), Byte.class, TypeCodec.tinyInt()},
                {DataType.cint(), Integer.class, TypeCodec.cint()},
                {DataType.bigint(), Long.class, TypeCodec.bigint()},
                {DataType.counter(), Long.class, TypeCodec.counter()},
                {DataType.cdouble(), Double.class, TypeCodec.cdouble()},
                {DataType.cfloat(), Float.class, TypeCodec.cfloat()},
                {DataType.varint(), BigInteger.class, TypeCodec.varint()},
                {DataType.decimal(), BigDecimal.class, TypeCodec.decimal()},
                {DataType.varchar(), String.class, TypeCodec.varchar()},
                {DataType.ascii(), String.class, TypeCodec.ascii()},
                {DataType.timestamp(), Date.class, TypeCodec.timestamp()},
                {DataType.date(), LocalDate.class, TypeCodec.date()},
                {DataType.time(), Long.class, TypeCodec.time()},
                {DataType.uuid(), UUID.class, TypeCodec.uuid()},
                {DataType.timeuuid(), UUID.class, TypeCodec.timeUUID()},
                {DataType.inet(), InetAddress.class, TypeCodec.inet()},
                {DataType.duration(), Duration.class, TypeCodec.duration()}
        };
    }

    @DataProvider
    public static Object[][] value() {
        return new Object[][]{
                {ByteBuffer.allocate(0), TypeCodec.blob()},
                {Boolean.TRUE, TypeCodec.cboolean()},
                {(short) 42, TypeCodec.smallInt()},
                {(byte) 42, TypeCodec.tinyInt()},
                {42, TypeCodec.cint()},
                {42L, TypeCodec.bigint()},
                {42D, TypeCodec.cdouble()},
                {42F, TypeCodec.cfloat()},
                {new BigInteger("1234"), TypeCodec.varint()},
                {new BigDecimal("123.45"), TypeCodec.decimal()},
                {"foo", TypeCodec.varchar()},
                {new Date(42), TypeCodec.timestamp()},
                {LocalDate.fromDaysSinceEpoch(42), TypeCodec.date()},
                {UUID.randomUUID(), TypeCodec.uuid()},
                {mock(InetAddress.class), TypeCodec.inet()},
                {Duration.from("1mo2d3h"), TypeCodec.duration()}
        };
    }

    @DataProvider
    public static Object[][] cqlAndValue() {
        return new Object[][]{
                {DataType.blob(), ByteBuffer.allocate(0), TypeCodec.blob()},
                {DataType.cboolean(), true, TypeCodec.cboolean()},
                {DataType.smallint(), (short) 42, TypeCodec.smallInt()},
                {DataType.tinyint(), (byte) 42, TypeCodec.tinyInt()},
                {DataType.cint(), 42, TypeCodec.cint()},
                {DataType.bigint(), 42L, TypeCodec.bigint()},
                {DataType.counter(), 42L, TypeCodec.counter()},
                {DataType.cdouble(), 42D, TypeCodec.cdouble()},
                {DataType.cfloat(), 42F, TypeCodec.cfloat()},
                {DataType.varint(), new BigInteger("1234"), TypeCodec.varint()},
                {DataType.decimal(), new BigDecimal("123.45"), TypeCodec.decimal()},
                {DataType.varchar(), "foo", TypeCodec.varchar()},
                {DataType.ascii(), "foo", TypeCodec.ascii()},
                {DataType.timestamp(), new Date(42), TypeCodec.timestamp()},
                {DataType.date(), LocalDate.fromDaysSinceEpoch(42), TypeCodec.date()},
                {DataType.time(), 42L, TypeCodec.time()},
                {DataType.uuid(), UUID.randomUUID(), TypeCodec.uuid()},
                {DataType.timeuuid(), UUID.randomUUID(), TypeCodec.timeUUID()},
                {DataType.inet(), mock(InetAddress.class), TypeCodec.inet()},
                {DataType.duration(), Duration.from("1mo2d3h"), TypeCodec.duration()}
        };
    }

    @Test(groups = "unit", dataProvider = "cql")
    public void should_find_codec_by_cql_type(DataType cqlType, TypeCodec<?> expected) {
        // given
        CodecRegistry registry = new CodecRegistry();
        // when
        TypeCodec<?> actual = registry.codecFor(cqlType);
        // then
        assertThat(actual)
                .isNotNull()
                .accepts(cqlType)
                .isSameAs(expected);
    }

    @Test(groups = "unit", dataProvider = "cqlAndJava")
    public void should_find_codec_by_cql_type_java_type(DataType cqlType, Class<?> javaType, TypeCodec<?> expected) {
        // given
        CodecRegistry registry = new CodecRegistry();
        // when
        TypeCodec<?> actual = registry.codecFor(cqlType, javaType);
        // then
        assertThat(actual)
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType)
                .isSameAs(expected);
    }

    @Test(groups = "unit", dataProvider = "value")
    public void should_find_codec_by_value(Object value, TypeCodec<?> expected) {
        // given
        CodecRegistry registry = new CodecRegistry();
        // when
        TypeCodec<?> actual = registry.codecFor(value);
        // then
        assertThat(actual)
                .isNotNull()
                .accepts(value)
                .isSameAs(expected);
    }

    @Test(groups = "unit", dataProvider = "cqlAndValue")
    public void should_find_codec_by_cql_type_and_value(DataType cqlType, Object value, TypeCodec<?> expected) {
        // given
        CodecRegistry registry = new CodecRegistry();
        // when
        TypeCodec<?> actual = registry.codecFor(cqlType, value);
        // then
        assertThat(actual)
                .isNotNull()
                .accepts(cqlType)
                .accepts(value)
                .isSameAs(expected);
    }

    @Test(groups = "unit")
    public void should_find_newly_registered_codec_by_cql_type() {
        // given
        CodecRegistry registry = new CodecRegistry();
        TypeCodec<?> expected = mockCodec(list(text()), listOf(String.class));
        registry.register(expected);
        // when
        TypeCodec<?> actual = registry.codecFor(list(text()));
        // then
        assertThat(actual)
                .isNotNull()
                .isSameAs(expected);
    }

    @Test(groups = "unit")
    public void should_find_default_codec_if_cql_type_already_registered() {
        // given
        CodecRegistry registry = new CodecRegistry();
        TypeCodec<?> newCodec = mockCodec(text(), of(StringBuilder.class));
        registry.register(newCodec);
        // when
        TypeCodec<?> actual = registry.codecFor(text());
        // then
        assertThat(actual)
                .isNotNull()
                .isNotSameAs(newCodec)
                .accepts(text())
                .accepts(String.class)
                .doesNotAccept(StringBuilder.class);
    }

    @Test(groups = "unit")
    public void should_find_newly_registered_codec_by_cql_type_and_java_type() {
        // given
        CodecRegistry registry = new CodecRegistry();
        TypeCodec<?> expected = mockCodec(list(text()), listOf(String.class));
        registry.register(expected);
        // when
        TypeCodec<?> actual = registry.codecFor(list(text()), listOf(String.class));
        // then
        assertThat(actual)
                .isNotNull()
                .isSameAs(expected);
    }

    @Test(groups = "unit")
    public void should_create_list_codec() {
        CollectionType cqlType = list(cint());
        TypeToken<List<Integer>> javaType = listOf(Integer.class);
        assertThat(new CodecRegistry().codecFor(cqlType))
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType);
        assertThat(new CodecRegistry().codecFor(cqlType, javaType))
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType);
        assertThat(new CodecRegistry().codecFor(singletonList(42)))
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType);
        assertThat(new CodecRegistry().codecFor(cqlType, singletonList(42)))
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType);
        assertThat(new CodecRegistry().codecFor(new ArrayList<Integer>()))
                .isNotNull()
                        // empty collections are mapped to blob codec if no CQL type provided
                .accepts(list(blob()))
                .accepts(listOf(ByteBuffer.class));
        assertThat(new CodecRegistry().codecFor(cqlType, new ArrayList<Integer>()))
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType);
    }

    @Test(groups = "unit")
    public void should_create_set_codec() {
        CollectionType cqlType = set(cint());
        TypeToken<Set<Integer>> javaType = setOf(Integer.class);
        assertThat(new CodecRegistry().codecFor(cqlType))
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType);
        assertThat(new CodecRegistry().codecFor(cqlType, javaType))
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType);
        assertThat(new CodecRegistry().codecFor(singleton(42)))
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType);
        assertThat(new CodecRegistry().codecFor(cqlType, singleton(42)))
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType);
        assertThat(new CodecRegistry().codecFor(new HashSet<Integer>()))
                .isNotNull()
                        // empty collections are mapped to blob codec if no CQL type provided
                .accepts(set(blob()))
                .accepts(setOf(ByteBuffer.class));
        assertThat(new CodecRegistry().codecFor(cqlType, new HashSet<Integer>()))
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType);
    }

    @Test(groups = "unit")
    public void should_create_map_codec() {
        CollectionType cqlType = map(cint(), list(varchar()));
        TypeToken<Map<Integer, List<String>>> javaType = mapOf(of(Integer.class), listOf(String.class));
        assertThat(new CodecRegistry().codecFor(cqlType))
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType);
        assertThat(new CodecRegistry().codecFor(cqlType, javaType))
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType);
        assertThat(new CodecRegistry().codecFor(singletonMap(42, singletonList("foo"))))
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType);
        assertThat(new CodecRegistry().codecFor(cqlType, singletonMap(42, singletonList("foo"))))
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType);
        assertThat(new CodecRegistry().codecFor(new HashMap<Integer, List<String>>()))
                .isNotNull()
                        // empty collections are mapped to blob codec if no CQL type provided
                .accepts(map(blob(), blob()))
                .accepts(mapOf(ByteBuffer.class, ByteBuffer.class));
        assertThat(new CodecRegistry().codecFor(cqlType, new HashMap<Integer, List<String>>()))
                .isNotNull()
                .accepts(cqlType)
                .accepts(javaType);
    }

    @Test(groups = "unit")
    public void should_create_tuple_codec() {
        CodecRegistry registry = new CodecRegistry();
        TupleType tupleType = TupleType.of(V4, registry, cint(), varchar());
        assertThat(registry.codecFor(tupleType))
                .isNotNull()
                .accepts(tupleType)
                .accepts(TupleValue.class);
        registry = new CodecRegistry();
        tupleType = TupleType.of(V4, registry, cint(), varchar());
        assertThat(registry.codecFor(tupleType, TupleValue.class))
                .isNotNull()
                .accepts(tupleType)
                .accepts(TupleValue.class);
        registry = new CodecRegistry();
        tupleType = TupleType.of(V4, registry, cint(), varchar());
        assertThat(registry.codecFor(new TupleValue(tupleType)))
                .isNotNull()
                .accepts(tupleType)
                .accepts(TupleValue.class);
        assertThat(registry.codecFor(tupleType, new TupleValue(tupleType)))
                .isNotNull()
                .accepts(tupleType)
                .accepts(TupleValue.class);
    }

    @Test(groups = "unit")
    public void should_create_udt_codec() {
        CodecRegistry registry = new CodecRegistry();
        UserType udt = new UserType("ks", "test", false, Collections.<UserType.Field>emptyList(), V4, registry);
        assertThat(registry.codecFor(udt))
                .isNotNull()
                .accepts(udt)
                .accepts(UDTValue.class);
        registry = new CodecRegistry();
        udt = new UserType("ks", "test", false, Collections.<UserType.Field>emptyList(), V4, registry);
        assertThat(registry.codecFor(udt, UDTValue.class))
                .isNotNull()
                .accepts(udt)
                .accepts(UDTValue.class);
        registry = new CodecRegistry();
        udt = new UserType("ks", "test", false, Collections.<UserType.Field>emptyList(), V4, registry);
        assertThat(registry.codecFor(new UDTValue(udt)))
                .isNotNull()
                .accepts(udt)
                .accepts(UDTValue.class);
        registry = new CodecRegistry();
        udt = new UserType("ks", "test", false, Collections.<UserType.Field>emptyList(), V4, registry);
        assertThat(registry.codecFor(udt, new UDTValue(udt)))
                .isNotNull()
                .accepts(udt)
                .accepts(UDTValue.class);
    }

    @Test(groups = "unit")
    public void should_create_codec_for_custom_cql_type() {
        DataType custom = DataType.custom("foo");
        assertThat(new CodecRegistry().codecFor(custom))
                .isNotNull()
                .accepts(custom)
                .accepts(ByteBuffer.class);
        assertThat(new CodecRegistry().codecFor(custom, ByteBuffer.class))
                .isNotNull()
                .accepts(custom)
                .accepts(ByteBuffer.class);
        assertThat(new CodecRegistry().codecFor(custom, ByteBuffer.allocate(0)))
                .isNotNull()
                .accepts(custom)
                .accepts(ByteBuffer.class);
    }

    @Test(groups = "unit")
    public void should_create_derived_codecs_for_java_type_handled_by_custom_codec() {
        TypeCodec<?> newCodec = mockCodec(varchar(), of(StringBuilder.class));
        CodecRegistry registry = new CodecRegistry().register(newCodec);
        // lookup by CQL type only returns default codec
        assertThat(registry.codecFor(list(varchar()))).doesNotAccept(listOf(StringBuilder.class));
        assertThat(registry.codecFor(list(varchar()), listOf(StringBuilder.class))).isNotNull();
    }

    @Test(groups = "unit")
    public void should_not_find_codec_if_java_type_unknown() {
        try {
            new CodecRegistry().codecFor(StringBuilder.class);
            fail("Should not have found a codec for ANY <-> StringBuilder");
        } catch (CodecNotFoundException e) {
            // expected
        }
        try {
            new CodecRegistry().codecFor(varchar(), StringBuilder.class);
            fail("Should not have found a codec for varchar <-> StringBuilder");
        } catch (CodecNotFoundException e) {
            // expected
        }
        try {
            new CodecRegistry().codecFor(new StringBuilder());
            fail("Should not have found a codec for ANY <-> StringBuilder");
        } catch (CodecNotFoundException e) {
            // expected
        }
        try {
            new CodecRegistry().codecFor(varchar(), new StringBuilder());
            fail("Should not have found a codec for varchar <-> StringBuilder");
        } catch (CodecNotFoundException e) {
            // expected
        }
    }

    @Test(groups = "unit")
    public void should_ignore_codec_colliding_with_already_registered_codec() {
        MemoryAppender logs = startCapturingLogs();

        CodecRegistry registry = new CodecRegistry();

        TypeCodec<?> newCodec = mockCodec(cint(), of(Integer.class));

        registry.register(newCodec);

        assertThat(logs.getNext()).contains("Ignoring codec MockCodec");
        assertThat(
                registry.codecFor(cint(), Integer.class)
        ).isNotSameAs(newCodec);

        stopCapturingLogs(logs);
    }

    @Test(groups = "unit")
    public void should_ignore_codec_colliding_with_already_generated_codec() {
        MemoryAppender logs = startCapturingLogs();

        CodecRegistry registry = new CodecRegistry();

        // Force generation of a list token from the default token
        registry.codecFor(list(cint()), listOf(Integer.class));

        TypeCodec<?> newCodec = mockCodec(list(cint()), listOf(Integer.class));

        registry.register(newCodec);

        assertThat(logs.getNext()).contains("Ignoring codec MockCodec");
        assertThat(
                registry.codecFor(list(cint()), listOf(Integer.class))
        ).isNotSameAs(newCodec);

        stopCapturingLogs(logs);
    }

    private MemoryAppender startCapturingLogs() {
        Logger registryLogger = Logger.getLogger(CodecRegistry.class);
        registryLogger.setLevel(Level.WARN);
        MemoryAppender logs = new MemoryAppender();
        registryLogger.addAppender(logs);
        return logs;
    }

    private void stopCapturingLogs(MemoryAppender logs) {
        Logger registryLogger = Logger.getLogger(CodecRegistry.class);
        registryLogger.setLevel(null);
        registryLogger.removeAppender(logs);
    }

    private <T> TypeCodec<T> mockCodec(DataType cqlType, TypeToken<T> javaType) {
        @SuppressWarnings("unchecked")
        TypeCodec<T> newCodec = mock(TypeCodec.class);
        when(newCodec.getCqlType()).thenReturn(cqlType);
        when(newCodec.getJavaType()).thenReturn(javaType);
        when(newCodec.accepts(cqlType)).thenReturn(true);
        when(newCodec.accepts(javaType)).thenReturn(true);
        when(newCodec.toString()).thenReturn(String.format("MockCodec [%s <-> %s]", cqlType, javaType));
        return newCodec;
    }

}
