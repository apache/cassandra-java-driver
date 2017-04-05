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

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * DataType simple unit tests.
 */
public class DataTypeTest {

    CodecRegistry codecRegistry = new CodecRegistry();

    ProtocolVersion protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;

    static boolean exclude(DataType t) {
        return t.getName() == DataType.Name.COUNTER || t.getName() == DataType.Name.DURATION;
    }

    /**
     * A test value for a primitive data type
     */
    static class TestValue {
        /**
         * The value as a Java object
         */
        final Object javaObject;
        /**
         * A CQL string that should parse to the value
         */
        final String cqlInputString;
        /**
         * How the value should be formatted in CQL
         */
        final String cqlOutputString;

        TestValue(Object javaObject, String cqlInputString, String cqlOutputString) {
            this.javaObject = javaObject;
            this.cqlInputString = cqlInputString;
            this.cqlOutputString = cqlOutputString;
        }
    }

    private static TestValue[] primitiveTestValues(DataType dt) {
        switch (dt.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return new TestValue[]{
                        new TestValue("foo", "'foo'", "'foo'"),
                        new TestValue("fo'o", "'fo''o'", "'fo''o'"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            case BIGINT:
                return new TestValue[]{
                        new TestValue(42L, "42", "42"),
                        new TestValue(91294377723L, "91294377723", "91294377723"),
                        new TestValue(-133L, "-133", "-133"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            case TIMESTAMP:
                // input: single quotes are optional for long literals, mandatory for date patterns
                return new TestValue[]{
                        new TestValue(new Date(42L), "42", "42"),
                        new TestValue(new Date(91294377723L), "91294377723", "91294377723"),
                        new TestValue(new Date(-133L), "-133", "-133"),
                        new TestValue(new Date(784041330999L), "'1994-11-05T14:15:30.999+0100'", "784041330999"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            case DATE:
                // input: single quotes are optional for long literals, mandatory for date patterns
                return new TestValue[]{
                        new TestValue(LocalDate.fromDaysSinceEpoch(16071), "'2014-01-01'", "'2014-01-01'"),
                        new TestValue(LocalDate.fromDaysSinceEpoch(0), "'1970-01-01'", "'1970-01-01'"),
                        new TestValue(LocalDate.fromDaysSinceEpoch((int) (2147483648L - (1L << 31))), "'2147483648'", "'1970-01-01'"),
                        new TestValue(LocalDate.fromDaysSinceEpoch((int) (0 - (1L << 31))), "0", "'-5877641-06-23'"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            case TIME:
                // input: all literals must by enclosed in single quotes
                return new TestValue[]{
                        new TestValue(54012123450000L, "'54012123450000'", "'15:00:12.123450000'"),
                        new TestValue(0L, "'0'", "'00:00:00.000000000'"),
                        new TestValue(54012012345000L, "'15:00:12.012345000'", "'15:00:12.012345000'"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            case BLOB:
                return new TestValue[]{
                        new TestValue(Bytes.fromHexString("0x2450"), "0x2450", "0x2450"),
                        new TestValue(ByteBuffer.allocate(0), "0x", "0x"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            case BOOLEAN:
                return new TestValue[]{
                        new TestValue(true, "true", "true"),
                        new TestValue(false, "false", "false"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            case DECIMAL:
                return new TestValue[]{
                        new TestValue(new BigDecimal("1.23E+8"), "1.23E+8", "1.23E+8"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            case DOUBLE:
                return new TestValue[]{
                        new TestValue(2.39324324, "2.39324324", "2.39324324"),
                        new TestValue(-12., "-12.0", "-12.0"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            case FLOAT:
                return new TestValue[]{
                        new TestValue(2.39f, "2.39", "2.39"),
                        new TestValue(-12.f, "-12.0", "-12.0"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            case INET:
                try {
                    return new TestValue[]{
                            new TestValue(InetAddress.getByName("128.2.12.3"), "'128.2.12.3'", "'128.2.12.3'"),
                            new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
                } catch (java.net.UnknownHostException e) {
                    throw new RuntimeException();
                }
            case TINYINT:
                return new TestValue[]{
                        new TestValue((byte) -4, "-4", "-4"),
                        new TestValue((byte) 44, "44", "44"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            case SMALLINT:
                return new TestValue[]{
                        new TestValue((short) -3, "-3", "-3"),
                        new TestValue((short) 43, "43", "43"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            case INT:
                return new TestValue[]{
                        new TestValue(-2, "-2", "-2"),
                        new TestValue(42, "42", "42"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            case TIMEUUID:
                return new TestValue[]{
                        new TestValue(UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66"), "fe2b4360-28c6-11e2-81c1-0800200c9a66", "fe2b4360-28c6-11e2-81c1-0800200c9a66"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            case UUID:
                return new TestValue[]{
                        new TestValue(UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66"), "fe2b4360-28c6-11e2-81c1-0800200c9a66", "fe2b4360-28c6-11e2-81c1-0800200c9a66"),
                        new TestValue(UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff00"), "067e6162-3b6f-4ae2-a171-2470b63dff00", "067e6162-3b6f-4ae2-a171-2470b63dff00"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            case VARINT:
                return new TestValue[]{
                        new TestValue(new BigInteger("12387290982347987032483422342432"), "12387290982347987032483422342432", "12387290982347987032483422342432"),
                        new TestValue(null, null, "NULL"), new TestValue(null, "null", "NULL"), new TestValue(null, "NULL", "NULL")};
            default:
                throw new RuntimeException("Missing handling of " + dt);
        }
    }

    @Test(groups = "unit")
    public void parseNativeTest() {
        for (DataType dt : DataType.allPrimitiveTypes()) {
            if (exclude(dt))
                continue;

            for (TestValue value : primitiveTestValues(dt))
                assertThat(codecRegistry.codecFor(dt).parse(value.cqlInputString))
                        .as("Parsing input %s to a %s", value.cqlInputString, dt)
                        .isEqualTo(value.javaObject);
        }
    }

    @Test(groups = "unit")
    public void formatNativeTest() {
        for (DataType dt : DataType.allPrimitiveTypes()) {
            if (exclude(dt))
                continue;

            for (TestValue value : primitiveTestValues(dt))
                assertThat(codecRegistry.codecFor(dt).format(value.javaObject))
                        .as("Formatting a %s expecting %s", dt, value.cqlOutputString)
                        .isEqualTo(value.cqlOutputString);
        }
    }

    @Test(groups = "unit")
    public void parseFormatListTest() {
        String toParse = "['Foo','Bar','Foo''bar']";
        List<String> toFormat = Arrays.asList("Foo", "Bar", "Foo'bar");
        DataType dt = DataType.list(DataType.text());
        assertEquals(codecRegistry.codecFor(dt).parse(toParse), toFormat);
        assertEquals(codecRegistry.codecFor(dt).format(toFormat), toParse);
    }

    @SuppressWarnings("serial")
    @Test(groups = "unit")
    public void parseFormatSetTest() {
        String toParse = "{'Foo','Bar','Foo''bar'}";
        Set<String> toFormat = new LinkedHashSet<String>() {{
            add("Foo");
            add("Bar");
            add("Foo'bar");
        }};
        DataType dt = DataType.set(DataType.text());
        assertEquals(codecRegistry.codecFor(dt).parse(toParse), toFormat);
        assertEquals(codecRegistry.codecFor(dt).format(toFormat), toParse);
    }

    @SuppressWarnings("serial")
    @Test(groups = "unit")
    public void parseFormatMapTest() {
        String toParse = "{'Foo':3,'Bar':42,'Foo''bar':-24}";
        Map<String, Integer> toFormat = new LinkedHashMap<String, Integer>() {{
            put("Foo", 3);
            put("Bar", 42);
            put("Foo'bar", -24);
        }};
        DataType dt = DataType.map(DataType.text(), DataType.cint());
        assertEquals(codecRegistry.codecFor(dt).parse(toParse), toFormat);
        assertEquals(codecRegistry.codecFor(dt).format(toFormat), toParse);
    }

    @SuppressWarnings("serial")
    @Test(groups = "unit")
    public void parseFormatUDTTest() {
        String toParse = "{t:'fo''o',i:3,\"L\":['a','b'],s:{3:{a:0x01}}}";

        final UserType udt1 = new UserType("ks", "t", false, Arrays.asList(new UserType.Field("a", DataType.blob())), protocolVersion, codecRegistry);
        UserType udt2 = new UserType("ks", "t", false, Arrays.asList(
                new UserType.Field("t", DataType.text()),
                new UserType.Field("i", DataType.cint()),
                new UserType.Field("L", DataType.list(DataType.text())),
                new UserType.Field("s", DataType.map(DataType.cint(), udt1))
        ), protocolVersion, codecRegistry);

        UDTValue toFormat = udt2.newValue();
        toFormat.setString("t", "fo'o");
        toFormat.setInt("i", 3);
        toFormat.setList("\"L\"", Arrays.<String>asList("a", "b"));
        toFormat.setMap("s", new HashMap<Integer, UDTValue>() {{
            put(3, udt1.newValue().setBytes("a", ByteBuffer.wrap(new byte[]{1})));
        }});

        assertEquals(codecRegistry.codecFor(udt2).parse(toParse), toFormat);
        assertEquals(codecRegistry.codecFor(udt2).format(toFormat), toParse);
    }

    @SuppressWarnings("deprecation")
    @Test(groups = "unit")
    public void parseFormatTupleTest() {

        String toParse = "(1,'foo',1.0)";
        TupleType t = new TupleType(newArrayList(DataType.cint(), DataType.text(), DataType.cfloat()), protocolVersion, codecRegistry);
        TupleValue toFormat = t.newValue(1, "foo", 1.0f);

        assertEquals(codecRegistry.codecFor(t).parse(toParse), toFormat);
        assertEquals(codecRegistry.codecFor(t).format(toFormat), toParse);
    }

    @Test(groups = "unit")
    public void serializeDeserializeTest() {
        for (ProtocolVersion v : ProtocolVersion.values())
            serializeDeserializeTest(v);
    }

    public void serializeDeserializeTest(ProtocolVersion version) {

        for (DataType dt : DataType.allPrimitiveTypes()) {
            if (exclude(dt))
                continue;

            Object value = TestUtils.getFixedValue(dt);
            TypeCodec<Object> codec = codecRegistry.codecFor(dt);
            assertEquals(codec.deserialize(codec.serialize(value, version), version), value);
        }

        TypeCodec<Long> codec = codecRegistry.codecFor(DataType.bigint());

        try {
            ByteBuffer badValue = ByteBuffer.allocate(4);
            codec.deserialize(badValue, version);
            fail("This should not have worked");
        } catch (InvalidTypeException e) { /* That's what we want */ }
    }

    @Test(groups = "unit")
    public void serializeDeserializeCollectionsTest() {
        for (ProtocolVersion v : ProtocolVersion.values())
            serializeDeserializeCollectionsTest(v);
    }

    public void serializeDeserializeCollectionsTest(ProtocolVersion version) {

        List<String> l = Arrays.asList("foo", "bar");

        DataType dt = DataType.list(DataType.text());
        TypeCodec<List<String>> codec = codecRegistry.codecFor(dt);
        assertEquals(codec.deserialize(codec.serialize(l, version), version), l);

        try {
            DataType listOfBigint = DataType.list(DataType.bigint());
            codec = codecRegistry.codecFor(listOfBigint);
            codec.serialize(l, version);
            fail("This should not have worked");
        } catch (InvalidTypeException e) { /* That's what we want */ }
    }
}
