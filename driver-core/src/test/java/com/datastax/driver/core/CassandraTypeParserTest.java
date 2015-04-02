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

import java.util.Arrays;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

public class CassandraTypeParserTest {

    @Test(groups = "unit")
    public void parseOneTest() {

        assertEquals(CassandraTypeParser.parseOne("org.apache.cassandra.db.marshal.InetAddressType"), DataType.inet());
        assertEquals(CassandraTypeParser.parseOne("org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)"), DataType.list(DataType.text()));
        assertEquals(CassandraTypeParser.parseOne("org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UTF8Type)"), DataType.text());

        String s;

        s = "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type, org.apache.cassandra.db.marshal.Int32Type)";
        assertEquals(CassandraTypeParser.parseOne(s), DataType.custom(s));

        s = "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))";
        assertEquals(CassandraTypeParser.parseOne(s), DataType.list(DataType.cint()));
    }

    @Test(groups = "unit")
    public void parseWithCompositeTest() {

        String s = "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.Int32Type, org.apache.cassandra.db.marshal.UTF8Type,";
              s += "org.apache.cassandra.db.marshal.ColumnToCollectionType(6162:org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)))";
        CassandraTypeParser.ParseResult r1 = CassandraTypeParser.parseWithComposite(s);
        assertTrue(r1.isComposite);
        assertEquals(r1.types, Arrays.asList(DataType.cint(), DataType.text()));
        assertEquals(r1.collections.size(), 1);
        assertEquals(r1.collections.get("ab"), DataType.list(DataType.cint()));

        CassandraTypeParser.ParseResult r2 = CassandraTypeParser.parseWithComposite("org.apache.cassandra.db.marshal.TimestampType");
        assertFalse(r2.isComposite);
        assertEquals(r2.types, Arrays.asList(DataType.timestamp()));
        assertEquals(r2.collections.size(), 0);
    }
}
