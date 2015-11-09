/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.net.InetAddresses;
import com.google.common.reflect.TypeToken;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.exceptions.InvalidTypeException;

import static com.datastax.driver.core.TypeTokens.listOf;
import static com.datastax.driver.core.TypeTokens.mapOf;
import static com.datastax.driver.core.TypeTokens.setOf;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.varchar;

public class SimpleStatementTest {

    SimpleStatement statement;

    ProtocolVersion protocolVersion = ProtocolVersion.V4;
    CodecRegistry codecRegistry;
    Token.Factory factory = Token.M3PToken.FACTORY;

    TupleType tupleType;
    TupleValue tupleValue;
    UserType userType;
    UDTValue udtValue;
    List<Integer> list = Lists.newArrayList(1, 2, 3);
    Set<String> set = Sets.newHashSet("a", "b", "c");
    Map<Integer, String> map = ImmutableMap.of(1, "a", 2, "b");
    InetAddress inet = InetAddresses.forString("127.0.0.1");
    UUID uuid = UUID.randomUUID();
    Token min = factory.fromString("-9223372036854775808");
    Token max = factory.fromString("4611686018427387904");

    @BeforeMethod(groups = "unit")
    public void setup() {
        codecRegistry = new CodecRegistry();

        tupleType = new TupleType(Lists.newArrayList(cint(), varchar()), protocolVersion, codecRegistry);
        tupleValue = new TupleValue(tupleType);

        userType = new UserType("ks", "udt1", Lists.newArrayList(new UserType.Field("f1", cint()), new UserType.Field("f2", varchar())), protocolVersion, codecRegistry);
        udtValue = new UDTValue(userType);

        statement = new SimpleStatement("doesn't matter", protocolVersion, codecRegistry);
    }

    @DataProvider(name = "SimpleStatementTest")
    public Object[][] parameters() throws UnknownHostException {
        return new Object[][] {
            { true, TypeToken.of(Boolean.class) },
            { (short)42, TypeToken.of(Short.class) },
            { (byte)42, TypeToken.of(Byte.class) },
            { 42, TypeToken.of(Integer.class) },
            { 42L, TypeToken.of(Long.class) },
            { 42.42f, TypeToken.of(Float.class) },
            { 42.42d, TypeToken.of(Double.class) },
            { new BigInteger("42"), TypeToken.of(BigInteger.class) },
            { new BigDecimal("42.42422"), TypeToken.of(BigDecimal.class) },
            { "foo", TypeToken.of(String.class) },
            { LocalDate.fromDaysSinceEpoch(42), TypeToken.of(LocalDate.class) },
            { new Date(42), TypeToken.of(Date.class) },
            { inet, TypeToken.of(InetAddress.class) },
            { uuid, TypeToken.of(UUID.class) },
            { list, listOf(Integer.class) },
            { set, setOf(String.class) },
            { map, mapOf(Integer.class, String.class) },
            { tupleValue, TypeToken.of(TupleValue.class) },
            { udtValue, TypeToken.of(UDTValue.class) }
        };
    }

    @Test(groups = "unit")
    public void should_set_and_retrieve_value_using_positional_parameters() {
        assertThat(statement.setBool(1, true).getBool(1)).isEqualTo(true);
        assertThat(statement.setShort(1, (short)42).getShort(1)).isEqualTo((short)42);
        assertThat(statement.setByte(1, (byte)42).getByte(1)).isEqualTo((byte)42);
        assertThat(statement.setInt(1, 42).getInt(1)).isEqualTo(42);
        assertThat(statement.setLong(1, 42L).getLong(1)).isEqualTo(42L);
        assertThat(statement.setFloat(1, 42.42f).getFloat(1)).isEqualTo(42.42f);
        assertThat(statement.setDouble(1, 42.42d).getDouble(1)).isEqualTo(42.42d);
        assertThat(statement.setVarint(1, new BigInteger("42")).getVarint(1)).isEqualTo(new BigInteger("42"));
        assertThat(statement.setDecimal(1, new BigDecimal("42.42422")).getDecimal(1)).isEqualTo(new BigDecimal("42.42422"));
        assertThat(statement.setString(1, "foo").getString(1)).isEqualTo("foo");
        assertThat(statement.setDate(1, LocalDate.fromDaysSinceEpoch(42)).getDate(1)).isEqualTo(LocalDate.fromDaysSinceEpoch(42));
        assertThat(statement.setTime(1, 123456).getTime(1)).isEqualTo(123456);
        assertThat(statement.setTimestamp(1, new Date(42)).getTimestamp(1)).isEqualTo(new Date(42));
        assertThat(statement.setInet(1, inet).getInet(1)).isEqualTo(inet);
        assertThat(statement.setUUID(1, uuid).getUUID(1)).isEqualTo(uuid);
        assertThat(statement.setList(1, list).getList(1, Integer.class)).isEqualTo(list);
        assertThat(statement.setList(1, list, Integer.class).getList(1, Integer.class)).isEqualTo(list);
        assertThat(statement.setSet(1, set).getSet(1, String.class)).isEqualTo(set);
        assertThat(statement.setSet(1, set, String.class).getSet(1, String.class)).isEqualTo(set);
        assertThat(statement.setMap(1, map).getMap(1, Integer.class, String.class)).isEqualTo(map);
        assertThat(statement.setMap(1, map, Integer.class, String.class).getMap(1, Integer.class, String.class)).isEqualTo(map);
        assertThat(statement.setTupleValue(1, tupleValue).getTupleValue(1)).isEqualTo(tupleValue);
        assertThat(statement.setUDTValue(1, udtValue).getUDTValue(1)).isEqualTo(udtValue);
        assertThat(statement.isSet(1)).isTrue();
        statement.unset(1);
        assertThat(statement.isSet(1)).isFalse();
    }

    @Test(groups = "unit")
    public void should_set_and_retrieve_value_using_named_parameters() {
        assertThat(statement.setBool("foo", true).getBool("foo")).isEqualTo(true);
        assertThat(statement.setShort("foo", (short)42).getShort("foo")).isEqualTo((short)42);
        assertThat(statement.setByte("foo", (byte)42).getByte("foo")).isEqualTo((byte)42);
        assertThat(statement.setInt("foo", 42).getInt("foo")).isEqualTo(42);
        assertThat(statement.setLong("foo", 42L).getLong("foo")).isEqualTo(42L);
        assertThat(statement.setFloat("foo", 42.42f).getFloat("foo")).isEqualTo(42.42f);
        assertThat(statement.setDouble("foo", 42.42d).getDouble("foo")).isEqualTo(42.42d);
        assertThat(statement.setVarint("foo", new BigInteger("42")).getVarint("foo")).isEqualTo(new BigInteger("42"));
        assertThat(statement.setDecimal("foo", new BigDecimal("42.42422")).getDecimal("foo")).isEqualTo(new BigDecimal("42.42422"));
        assertThat(statement.setString("foo", "foo").getString("foo")).isEqualTo("foo");
        assertThat(statement.setDate("foo", LocalDate.fromDaysSinceEpoch(42)).getDate("foo")).isEqualTo(LocalDate.fromDaysSinceEpoch(42));
        assertThat(statement.setTime("foo", 123456).getTime("foo")).isEqualTo(123456);
        assertThat(statement.setTimestamp("foo", new Date(42)).getTimestamp("foo")).isEqualTo(new Date(42));
        assertThat(statement.setInet("foo", inet).getInet("foo")).isEqualTo(inet);
        assertThat(statement.setUUID("foo", uuid).getUUID("foo")).isEqualTo(uuid);
        assertThat(statement.setList("foo", list).getList("foo", Integer.class)).isEqualTo(list);
        assertThat(statement.setList("foo", list, Integer.class).getList("foo", Integer.class)).isEqualTo(list);
        assertThat(statement.setSet("foo", set).getSet("foo", String.class)).isEqualTo(set);
        assertThat(statement.setSet("foo", set, String.class).getSet("foo", String.class)).isEqualTo(set);
        assertThat(statement.setMap("foo", map).getMap("foo", Integer.class, String.class)).isEqualTo(map);
        assertThat(statement.setMap("foo", map, Integer.class, String.class).getMap("foo", Integer.class, String.class)).isEqualTo(map);
        assertThat(statement.setTupleValue("foo", tupleValue).getTupleValue("foo")).isEqualTo(tupleValue);
        assertThat(statement.setUDTValue("foo", udtValue).getUDTValue("foo")).isEqualTo(udtValue);
        assertThat(statement.isSet("foo")).isTrue();
        statement.unset("foo");
        assertThat(statement.isSet("foo")).isFalse();
    }

    @Test(groups = "unit", dataProvider = "SimpleStatementTest")
    public <T> void should_set_and_retrieve_value_using_positional_parameters(T expected, TypeToken<T> javaType) {
        assertThat(statement.set(1, expected, javaType).get(1, javaType)).isEqualTo(expected);
        assertThat(statement.set(1, expected, javaType).getObject(1)).isEqualTo(expected);
        assertThat(statement.setToNull(1).isNull(1)).isTrue();
        assertThat(statement.isSet(1)).isTrue();
        statement.unset(1);
        assertThat(statement.isSet(1)).isFalse();
    }

    @Test(groups = "unit", dataProvider = "SimpleStatementTest")
    public <T> void should_set_and_retrieve_value_using_named_parameters(T expected, TypeToken<T> javaType) {
        assertThat(statement.set("foo", expected, javaType).get("foo", javaType)).isEqualTo(expected);
        assertThat(statement.set("foo", expected, javaType).getObject("foo")).isEqualTo(expected);
        assertThat(statement.setToNull("foo").isNull("foo")).isTrue();
        assertThat(statement.isSet("foo")).isTrue();
        statement.unset("foo");
        assertThat(statement.isSet("foo")).isFalse();
    }

    @Test(groups = "unit")
    public void should_set_bytes_unsafe_using_positional_parameters(){
        statement.setBytesUnsafe(0, ByteBuffer.allocate(4).putInt(0, 42));
        assertThat(statement.getBytesUnsafe(0).getInt()).isEqualTo(42);
    }

    @Test(groups = "unit")
    public void should_set_bytes_unsafe_using_named_parameters(){
        statement.setBytesUnsafe("foo", ByteBuffer.allocate(4).putInt(0, 42));
        assertThat(statement.getBytesUnsafe("foo").getInt()).isEqualTo(42);
    }

    @Test(groups = "unit")
    public void should_get_and_set_using_different_types_if_compatible() {
        codecRegistry.register(new StringToCassandraIntCodec());
        statement.setInt("foo", 42);
        assertThat(statement.getString("foo")).isEqualTo("42");
    }

    @Test(groups = "unit")
    public void should_set_token_using_positional_parameters(){
        statement.setToken(0, min).setToken(1, max);
        assertThat(statement.getBytesUnsafe(0)).isEqualTo(min.serialize(protocolVersion));
        assertThat(statement.getBytesUnsafe(1)).isEqualTo(max.serialize(protocolVersion));
    }

    @Test(groups = "unit")
    public void should_set_token_using_named_parameters(){
        Token min = factory.fromString("-9223372036854775808");
        Token max = factory.fromString("4611686018427387904");
        statement.setToken("min", min).setToken("max", max);
        assertThat(statement.getBytesUnsafe("min")).isEqualTo(min.serialize(protocolVersion));
        assertThat(statement.getBytesUnsafe("max")).isEqualTo(max.serialize(protocolVersion));
    }

    @Test(groups = "unit")
    public void should_set_token_using_single_partition_key_token(){
        statement.setPartitionKeyToken(min);
        assertThat(statement.getBytesUnsafe("partition key token")).isEqualTo(min.serialize(protocolVersion));
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void should_fail_if_too_many_variables() {
        List<Object> args = Collections.nCopies(1 << 16, (Object)1);
        statement.bind(args.toArray());
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void should_throw_IAE_if_getObject_called_on_statement_without_values() {
        statement.getObject(0);
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void should_throw_IAE_if_getObject_called_with_wrong_index() {
        statement.bind(new Date()).getObject(1);
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void should_throw_IAE_if_positional_and_named_parameters_are_both_used() {
        statement.set("foo", null, Date.class).setToNull(1);
    }
    
    @Test(groups = "unit")
    public void should_return_number_of_values() {
        assertThat(
            new SimpleStatement("doesn't matter", protocolVersion, codecRegistry).valuesCount()
        ).isEqualTo(0);
        assertThat(
            new SimpleStatement("doesn't matter", protocolVersion, codecRegistry, 1, 2).valuesCount()
        ).isEqualTo(2);
    }

    private static class StringToCassandraIntCodec extends TypeCodec<String>  {

        PrimitiveIntCodec intCodec = TypeCodec.cint();

        StringToCassandraIntCodec() {
            super(DataType.cint(), String.class);
        }

        @Override
        public ByteBuffer serialize(String value, ProtocolVersion protocolVersion) throws InvalidTypeException {
            int i = Integer.parseInt(value);
            return intCodec.serialize(i, protocolVersion);
        }

        @Override
        public String deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
            Integer i = intCodec.deserialize(bytes, protocolVersion);
            return i.toString();
        }

        @Override
        public String parse(String value) throws InvalidTypeException {
            throw new UnsupportedOperationException("not covered in this test");
        }

        @Override
        public String format(String value) throws InvalidTypeException {
            throw new UnsupportedOperationException("not covered in this test");
        }
    }

}
