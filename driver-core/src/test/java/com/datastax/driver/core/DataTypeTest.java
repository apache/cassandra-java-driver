/*
 *      Copyright (C) 2012 DataStax Inc.
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

import java.nio.ByteBuffer;
import java.util.*;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * DataType simple unit tests.
 */
public class DataTypeTest {

    @Test(groups = "unit")
    public void udtParsingTest() {

        UDTDefinition udt1 = new UDTDefinition("ks", "t", Arrays.asList(new UDTDefinition.Field("a", DataType.blob())));

        UDTDefinition udt2 = new UDTDefinition("ks", "t", Arrays.asList(
            new UDTDefinition.Field("t", DataType.text()),
            new UDTDefinition.Field("i", DataType.cint()),
            new UDTDefinition.Field("l", DataType.list(DataType.text())),
            new UDTDefinition.Field("s", DataType.map(DataType.cint(), DataType.userType(udt1)))
        ));

        DataType u = DataType.userType(udt2);
        UDTValue v = (UDTValue)u.parse("{ i : 3, t : 'fo''o', l: ['a', 'b'], s: { 3 : {a : 0x01}}}");

        assertEquals(v.getInt("i"), 3);
        assertEquals(v.getString("t"), "fo'o");
        assertEquals(v.getList("l", String.class), Arrays.<String>asList("a", "b"));

        UDTValue expected = udt1.newValue().setBytes("a", ByteBuffer.wrap(new byte[]{1}));
        Map<Integer, UDTValue> m = v.getMap("s", Integer.class, UDTValue.class);
        assertEquals(m.get(3), expected);
    }

    @Test(groups = "unit")
    public void serializeDeserializeTest() {
        for (int v = 1; v <= ProtocolOptions.NEWEST_SUPPORTED_PROTOCOL_VERSION; v++)
            serializeDeserializeTest(v);
    }

    public void serializeDeserializeTest(int version) {

        for (DataType dt : DataType.allPrimitiveTypes())
        {
            if (dt.getName() == DataType.Name.COUNTER)
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
        for (int v = 1; v <= ProtocolOptions.NEWEST_SUPPORTED_PROTOCOL_VERSION; v++)
            serializeDeserializeCollectionsTest(v);
    }

    public void serializeDeserializeCollectionsTest(int version) {

        List<String> l = Arrays.asList("foo", "bar");

        DataType dt = DataType.list(DataType.text());
        assertEquals(dt.deserialize(dt.serialize(l, version), version), l);

        try {
            DataType.list(DataType.bigint()).serialize(l, version);
            fail("This should not have worked");
        } catch (InvalidTypeException e) { /* That's what we want */ }
    }
}
