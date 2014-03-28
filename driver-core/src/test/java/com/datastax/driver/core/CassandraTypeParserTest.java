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

import java.util.Arrays;
import java.util.Iterator;

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

    @Test(groups = "unit")
    public void parseUserTypes() {

        String s = "org.apache.cassandra.db.marshal.UserType(foo,61646472657373,737472656574:org.apache.cassandra.db.marshal.UTF8Type,7a6970636f6465:org.apache.cassandra.db.marshal.Int32Type,70686f6e6573:org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UserType(foo,70686f6e65,6e616d65:org.apache.cassandra.db.marshal.UTF8Type,6e756d626572:org.apache.cassandra.db.marshal.UTF8Type)))";
        UDTDefinition def = CassandraTypeParser.parseOne(s).getUDTDefinition();

        assertEquals(def.getKeyspace(), "foo");
        assertEquals(def.getName(), "address");

        Iterator<UDTDefinition.Field> iter = def.iterator();

        UDTDefinition.Field field1 = iter.next();
        assertEquals(field1.getName(), "street");
        assertEquals(field1.getType(), DataType.text());

        UDTDefinition.Field field2 = iter.next();
        assertEquals(field2.getName(), "zipcode");
        assertEquals(field2.getType(), DataType.cint());

        UDTDefinition.Field field3 = iter.next();
        assertEquals(field3.getName(), "phones");

        DataType st = field3.getType();
        assertEquals(st.getName(), DataType.Name.SET);
        UDTDefinition subDef = st.getTypeArguments().get(0).getUDTDefinition();

        assertEquals(subDef.getKeyspace(), "foo");
        assertEquals(subDef.getName(), "phone");

        Iterator<UDTDefinition.Field> subIter = subDef.iterator();

        UDTDefinition.Field subField1 = subIter.next();
        assertEquals(subField1.getName(), "name");
        assertEquals(subField1.getType(), DataType.text());

        UDTDefinition.Field subField2 = subIter.next();
        assertEquals(subField2.getName(), "number");
        assertEquals(subField2.getType(), DataType.text());
    }
}
