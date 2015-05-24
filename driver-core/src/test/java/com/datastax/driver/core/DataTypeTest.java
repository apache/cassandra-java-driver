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
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.reflect.TypeToken;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;

import static com.datastax.driver.core.Assertions.assertThat;

/**
 * DataType simple unit tests.
 */
public class DataTypeTest {

    static boolean exclude(DataType t) {
        return t.getName() == DataType.Name.COUNTER;
    }

    private static String[] getCQLStringTestData(DataType dt) {
        switch (dt.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return new String[]{ "'foo'", "'fo''o'" };
            case BIGINT:
            case TIMESTAMP:
                return new String[]{ "42", "91294377723", "-133" };
            case BLOB:
                return new String[]{ "0x2450", "0x" };
            case BOOLEAN:
                return new String[]{ "true", "false" };
            case DECIMAL:
                return new String[]{ "1.23E+8" };
            case DOUBLE:
                return new String[]{ "2.39324324", "-12.0" };
            case FLOAT:
                return new String[]{ "2.39", "-12.0" };
            case INET:
                return new String[]{ "'128.2.12.3'" };
            case TINYINT:
                return new String[]{ "-4", "44" };
            case SMALLINT:
                return new String[]{ "-3", "43" };
            case INT:
                return new String[]{ "-2", "42" };
            case TIMEUUID:
                return new String[]{ "fe2b4360-28c6-11e2-81c1-0800200c9a66" };
            case UUID:
                return new String[]{ "fe2b4360-28c6-11e2-81c1-0800200c9a66", "067e6162-3b6f-4ae2-a171-2470b63dff00" };
            case VARINT:
                return new String[]{ "12387290982347987032483422342432" };
            default:
                throw new RuntimeException("Missing handling of " + dt);
        }
    }

    private static Object[] getTestData(DataType dt) {
        switch (dt.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return new Object[]{ "foo", "fo'o" };
            case BIGINT:
                return new Object[]{ 42L, 91294377723L, -133L };
            case TIMESTAMP:
                return new Object[]{ new Date(42L), new Date(91294377723L), new Date(-133L) };
            case BLOB:
                return new Object[]{ Bytes.fromHexString("0x2450"), ByteBuffer.allocate(0) };
            case BOOLEAN:
                return new Object[]{ true, false };
            case DECIMAL:
                return new Object[]{ new BigDecimal("1.23E+8") };
            case DOUBLE:
                return new Object[]{ 2.39324324, -12. };
            case FLOAT:
                return new Object[]{ 2.39f, -12.f };
            case INET:
                try {
                    return new Object[]{ InetAddress.getByName("128.2.12.3") };
                } catch (java.net.UnknownHostException e) {
                    throw new RuntimeException();
                }
            case TINYINT:
                return new Object[]{ (byte)-4, (byte)44 };
            case SMALLINT:
                return new Object[]{ (short)-3, (short)43 };
            case INT:
                return new Object[]{ -2, 42 };
            case TIMEUUID:
                return new Object[]{ UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66") };
            case UUID:
                return new Object[]{ UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66"), UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff00") };
            case VARINT:
                return new Object[]{ new BigInteger("12387290982347987032483422342432") };
            default:
                throw new RuntimeException("Missing handling of " + dt);
        }
    }

    @Test(groups = "unit")
    public void parseNativeTest() {
        for (DataType dt : DataType.allPrimitiveTypes()) {
            if (exclude(dt))
                continue;

            String[] s = getCQLStringTestData(dt);
            Object[] o = getTestData(dt);
            for (int i = 0; i < s.length; i++)
                assertEquals(dt.parse(s[i]), o[i], String.format("For input %d of %s, ", i, dt, s[i], o[i]));
        }
    }

    @Test(groups = "unit")
    public void formatNativeTest() {
        for (DataType dt : DataType.allPrimitiveTypes()) {
            if (exclude(dt))
                continue;

            String[] s = getCQLStringTestData(dt);
            Object[] o = getTestData(dt);
            for (int i = 0; i < s.length; i++)
                assertEquals(dt.format(o[i]), s[i], String.format("For input %d of %s, ", i, dt, o[i], s[i]));
        }
    }

    @Test(groups = "unit")
    public void parseFormatListTest() {
        String toParse = "['Foo', 'Bar', 'Foo''bar']";
        List<String> toFormat = Arrays.asList("Foo", "Bar", "Foo'bar");
        DataType dt = DataType.list(DataType.text());
        assertEquals(dt.parse(toParse), toFormat);
        assertEquals(dt.format(toFormat), toParse);
    }

    @SuppressWarnings("serial")
    @Test(groups = "unit")
    public void parseFormatSetTest() {
        String toParse = "{'Foo', 'Bar', 'Foo''bar'}";
        Set<String> toFormat = new LinkedHashSet<String>(){{ add("Foo"); add("Bar"); add("Foo'bar"); }};
        DataType dt = DataType.set(DataType.text());
        assertEquals(dt.parse(toParse), toFormat);
        assertEquals(dt.format(toFormat), toParse);
    }

    @SuppressWarnings("serial")
    @Test(groups = "unit")
    public void parseFormatMapTest() {
        String toParse = "{'Foo':3, 'Bar':42, 'Foo''bar':-24}";
        Map<String, Integer> toFormat = new LinkedHashMap<String, Integer>(){{ put("Foo", 3); put("Bar", 42); put("Foo'bar", -24); }};
        DataType dt = DataType.map(DataType.text(), DataType.cint());
        assertEquals(dt.parse(toParse), toFormat);
        assertEquals(dt.format(toFormat), toParse);
    }

    @SuppressWarnings("serial")
    @Test(groups = "unit")
    public void parseFormatUDTTest() {

        String toParse = "{t:'fo''o', i:3, l:['a', 'b'], s:{3:{a:0x01}}}";

        final UserType udt1 = new UserType("ks", "t", Arrays.asList(new UserType.Field("a", DataType.blob())));
        UserType udt2 = new UserType("ks", "t", Arrays.asList(
            new UserType.Field("t", DataType.text()),
            new UserType.Field("i", DataType.cint()),
            new UserType.Field("l", DataType.list(DataType.text())),
            new UserType.Field("s", DataType.map(DataType.cint(), udt1))
        ));

        UDTValue toFormat = udt2.newValue();
        toFormat.setString("t", "fo'o");
        toFormat.setInt("i", 3);
        toFormat.setList("l", Arrays.<String>asList("a", "b"));
        toFormat.setMap("s", new HashMap<Integer, UDTValue>(){{ put(3, udt1.newValue().setBytes("a", ByteBuffer.wrap(new byte[]{1}))); }});

        assertEquals(udt2.parse(toParse), toFormat);
        assertEquals(udt2.format(toFormat), toParse);
    }

    @Test(groups = "unit")
    public void parseFormatTupleTest() {

        String toParse = "(1, 'foo', 1.0)";
        TupleType t = TupleType.of(DataType.cint(), DataType.text(), DataType.cfloat());
        TupleValue toFormat = t.newValue(1, "foo", 1.0f);

        assertEquals(t.parse(toParse), toFormat);
        assertEquals(t.format(toFormat), toParse);
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
            assertEquals(dt.deserialize(dt.serialize(value, version), version), value);
        }

        try {
            DataType.bigint().serialize(4, version);
            fail("This should not have worked");
        } catch (InvalidTypeException e) { /* That's what we want */ }

        try {
            ByteBuffer badValue = ByteBuffer.allocate(4);
            DataType.bigint().deserialize(badValue, version);
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
        assertEquals(dt.deserialize(dt.serialize(l, version), version), l);

        try {
            DataType.list(DataType.bigint()).serialize(l, version);
            fail("This should not have worked");
        } catch (InvalidTypeException e) { /* That's what we want */ }
    }

    @Test(groups = "unit")
    public void should_check_which_java_type_it_can_be_deserialized_to() {
        assertThat(DataType.cint())
            .canBeDeserializedAs(TypeToken.of(Integer.class))
            .canBeDeserializedAs(TypeToken.of(Number.class))
            .cannotBeDeserializedAs(TypeToken.of(String.class));

        assertThat(DataType.list(DataType.cint()))
            .canBeDeserializedAs(new TypeToken<List<Integer>>(){})
            .canBeDeserializedAs(new TypeToken<Collection<Integer>>(){})
            .cannotBeDeserializedAs(new TypeToken<Set<Integer>>(){})
            .cannotBeDeserializedAs(new TypeToken<List<String>>(){});

        assertThat(DataType.map(DataType.cint(), DataType.frozenList(DataType.text())))
            .canBeDeserializedAs(new TypeToken<Map<Integer, List<String>>>(){})
            .canBeDeserializedAs(new TypeToken<Map<Number, Collection<String>>>(){})
            .cannotBeDeserializedAs(new TypeToken<Map<Integer, Collection<Integer>>>(){});
    }
}
