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

import java.util.*;
import java.nio.ByteBuffer;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Test we "support" custom types.
 */
public class CustomTypeTest extends CCMBridge.PerClassSingleNodeCluster {

    protected Collection<String> getTableDefinitions() {
        return Collections.singleton(
              "CREATE TABLE test ("
            + "    k int,"
            + "    c 'DynamicCompositeType(s => UTF8Type, i => Int32Type)',"
            + "    v int,"
            + "    PRIMARY KEY (k, c)"
            + ") WITH COMPACT STORAGE"
        );
    }

    private ByteBuffer serializeForDynamicType(Object... params) {

        List<ByteBuffer> l = new ArrayList<ByteBuffer>();
        int size = 0;
        for (Object p : params) {
            if (p instanceof Integer) {
                ByteBuffer elt = ByteBuffer.allocate(2 + 2 + 4 + 1);
                elt.putShort((short)(0x8000 | 'i'));
                elt.putShort((short) 4);
                elt.putInt((Integer)p);
                elt.put((byte)0);
                elt.flip();
                size += elt.remaining();
                l.add(elt);
            } else if (p instanceof String) {
                ByteBuffer bytes = ByteBuffer.wrap(((String)p).getBytes());
                ByteBuffer elt = ByteBuffer.allocate(2 + 2 + bytes.remaining() + 1);
                elt.putShort((short)(0x8000 | 's'));
                elt.putShort((short) bytes.remaining());
                elt.put(bytes);
                elt.put((byte)0);
                elt.flip();
                size += elt.remaining();
                l.add(elt);
            } else {
                throw new RuntimeException();
            }
        }
        ByteBuffer res = ByteBuffer.allocate(size);
        for (ByteBuffer bb : l)
            res.put(bb);
        res.flip();
        return res;
    }

    @Test(groups = "integration")
    public void DynamicCompositeTypeTest() {

        session.execute("INSERT INTO test(k, c, v) VALUES (0, 's@foo:i@32', 1)");
        session.execute("INSERT INTO test(k, c, v) VALUES (0, 'i@42', 2)");
        session.execute("INSERT INTO test(k, c, v) VALUES (0, 'i@12:i@3', 3)");

        ResultSet rs = session.execute("SELECT * FROM test");

        Row r = rs.one();
        assertEquals(r.getInt("k"), 0);
        assertEquals(r.getBytesUnsafe("c"), serializeForDynamicType(12, 3));
        assertEquals(r.getInt("v"), 3);

        r = rs.one();
        assertEquals(r.getInt("k"), 0);
        assertEquals(r.getBytesUnsafe("c"), serializeForDynamicType(42));
        assertEquals(r.getInt("v"), 2);

        r = rs.one();
        assertEquals(r.getInt("k"), 0);
        assertEquals(r.getBytesUnsafe("c"), serializeForDynamicType("foo", 32));
        assertEquals(r.getInt("v"), 1);
    }
}
