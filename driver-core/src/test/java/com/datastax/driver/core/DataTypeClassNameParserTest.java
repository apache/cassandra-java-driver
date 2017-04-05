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

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Iterator;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.testng.Assert.*;

public class DataTypeClassNameParserTest {

    private ProtocolVersion protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;
    private CodecRegistry codecRegistry = new CodecRegistry();

    @Test(groups = "unit")
    public void parseOneTest() {

        assertEquals(DataTypeClassNameParser.parseOne("org.apache.cassandra.db.marshal.ByteType", protocolVersion, codecRegistry), DataType.tinyint());
        assertEquals(DataTypeClassNameParser.parseOne("org.apache.cassandra.db.marshal.ShortType", protocolVersion, codecRegistry), DataType.smallint());
        assertEquals(DataTypeClassNameParser.parseOne("org.apache.cassandra.db.marshal.SimpleDateType", protocolVersion, codecRegistry), DataType.date());
        assertEquals(DataTypeClassNameParser.parseOne("org.apache.cassandra.db.marshal.TimeType", protocolVersion, codecRegistry), DataType.time());
        assertEquals(DataTypeClassNameParser.parseOne("org.apache.cassandra.db.marshal.InetAddressType", protocolVersion, codecRegistry), DataType.inet());
        assertEquals(DataTypeClassNameParser.parseOne("org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)", protocolVersion, codecRegistry), DataType.list(DataType.text()));
        assertEquals(DataTypeClassNameParser.parseOne("org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UTF8Type)", protocolVersion, codecRegistry), DataType.text());

        String s;

        s = "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type, org.apache.cassandra.db.marshal.Int32Type)";
        assertEquals(DataTypeClassNameParser.parseOne(s, protocolVersion, codecRegistry), DataType.custom(s));

        s = "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))";
        assertEquals(DataTypeClassNameParser.parseOne(s, protocolVersion, codecRegistry), DataType.list(DataType.cint()));
    }

    @Test(groups = "unit")
    public void parseWithCompositeTest() {

        String s = "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.Int32Type, org.apache.cassandra.db.marshal.UTF8Type,";
        s += "org.apache.cassandra.db.marshal.ColumnToCollectionType(6162:org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)))";
        DataTypeClassNameParser.ParseResult r1 = DataTypeClassNameParser.parseWithComposite(s, protocolVersion, codecRegistry);
        assertTrue(r1.isComposite);
        assertEquals(r1.types, Arrays.asList(DataType.cint(), DataType.text()));
        assertEquals(r1.collections.size(), 1);
        assertEquals(r1.collections.get("ab"), DataType.list(DataType.cint()));

        DataTypeClassNameParser.ParseResult r2 = DataTypeClassNameParser.parseWithComposite("org.apache.cassandra.db.marshal.TimestampType", protocolVersion, codecRegistry);
        assertFalse(r2.isComposite);
        assertEquals(r2.types, Arrays.asList(DataType.timestamp()));
        assertEquals(r2.collections.size(), 0);
    }

    @Test(groups = "unit")
    public void parseUserTypes() {

        String s = "org.apache.cassandra.db.marshal.UserType(foo,61646472657373,737472656574:org.apache.cassandra.db.marshal.UTF8Type,7a6970636f6465:org.apache.cassandra.db.marshal.Int32Type,70686f6e6573:org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UserType(foo,70686f6e65,6e616d65:org.apache.cassandra.db.marshal.UTF8Type,6e756d626572:org.apache.cassandra.db.marshal.UTF8Type)))";
        UserType def = (UserType) DataTypeClassNameParser.parseOne(s, protocolVersion, codecRegistry);

        assertEquals(def.getKeyspace(), "foo");
        assertEquals(def.getTypeName(), "address");

        Iterator<UserType.Field> iter = def.iterator();

        UserType.Field field1 = iter.next();
        assertEquals(field1.getName(), "street");
        assertEquals(field1.getType(), DataType.text());

        UserType.Field field2 = iter.next();
        assertEquals(field2.getName(), "zipcode");
        assertEquals(field2.getType(), DataType.cint());

        UserType.Field field3 = iter.next();
        assertEquals(field3.getName(), "phones");

        DataType st = field3.getType();
        assertEquals(st.getName(), DataType.Name.SET);
        UserType subDef = (UserType) st.getTypeArguments().get(0);

        assertEquals(subDef.getKeyspace(), "foo");
        assertEquals(subDef.getTypeName(), "phone");

        Iterator<UserType.Field> subIter = subDef.iterator();

        UserType.Field subField1 = subIter.next();
        assertEquals(subField1.getName(), "name");
        assertEquals(subField1.getType(), DataType.text());

        UserType.Field subField2 = subIter.next();
        assertEquals(subField2.getName(), "number");
        assertEquals(subField2.getType(), DataType.text());
    }

    @Test(groups = "unit")
    public void parseTupleTest() {
        String s = "org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.FloatType)";
        TupleType type = (TupleType) DataTypeClassNameParser.parseOne(s, protocolVersion, codecRegistry);
        assertNotNull(type);
        assertEquals(type.getComponentTypes().get(0), DataType.cint());
        assertEquals(type.getComponentTypes().get(1), DataType.text());
        assertEquals(type.getComponentTypes().get(2), DataType.cfloat());
    }

    @Test(groups = "unit")
    public void parseNestedCollectionTest() {
        // map<text, frozen<map<int,int>>>
        String s = "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.Int32Type)))";

        DataType parentMap = DataTypeClassNameParser.parseOne(s, protocolVersion, codecRegistry);
        assertThat(parentMap)
                .hasName(DataType.Name.MAP)
                .isNotFrozen()
                .hasTypeArgument(0, DataType.text());

        DataType childMap = parentMap.getTypeArguments().get(1);
        assertThat(childMap)
                .hasName(DataType.Name.MAP)
                .isFrozen()
                .hasTypeArguments(DataType.cint(), DataType.cint());
    }
}
