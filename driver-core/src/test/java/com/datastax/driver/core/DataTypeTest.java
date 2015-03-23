/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.datastax.driver.core.exceptions.InvalidTypeException;

public class DataTypeTest {

    @Test(groups = "unit")
    public void serializeDeserializeTest() {

        for (DataType dt : DataType.allPrimitiveTypes()) {
            if (dt.getName() == DataType.Name.COUNTER)
                continue;

            Object value = TestUtils.getFixedValue(dt);
            assertEquals(dt.deserialize(dt.serialize(value)), value);
        }

        try {
            DataType.bigint().serialize(4);
            fail("This should not have worked");
        } catch (InvalidTypeException e) { /* That's what we want */ }

        try {
            ByteBuffer badValue = ByteBuffer.allocate(4);
            DataType.bigint().deserialize(badValue);
            fail("This should not have worked");
        } catch (InvalidTypeException e) { /* That's what we want */ }
    }

    @Test(groups = "unit")
    public void serializeDeserializeCollectionsTest() {

        List<String> l = Arrays.asList("foo", "bar");

        DataType dt = DataType.list(DataType.text());
        assertEquals(dt.deserialize(dt.serialize(l)), l);

        try {
            DataType.list(DataType.bigint()).serialize(l);
            fail("This should not have worked");
        } catch (InvalidTypeException e) { /* That's what we want */ }
    }
}
