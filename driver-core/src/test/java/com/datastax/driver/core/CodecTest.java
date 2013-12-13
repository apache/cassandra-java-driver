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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.datastax.driver.core.DataType.*;
import org.apache.cassandra.db.marshal.*;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class CodecTest {

    public static final DataType CUSTOM_FOO = DataType.custom("com.example.FooBar");

    @Test(groups = "unit")
    public void testCustomList() throws Exception {
        ListType<?> listType = (ListType) Codec.getCodec(DataType.list(CUSTOM_FOO));
        Assert.assertNotNull(listType);
        Assert.assertTrue(listType.valueComparator() instanceof BytesType);
    }

    @Test(groups = "unit")
    public void testCustomSet() throws Exception {
        SetType<?> setType = (SetType) Codec.getCodec(DataType.set(CUSTOM_FOO));
        Assert.assertNotNull(setType);
        Assert.assertEquals(setType.nameComparator().getClass(),BytesType.class);
    }

    @Test(groups = "unit")
    public void testCustomKeyMap() throws Exception {
        MapType<?, ?> mapType = (MapType) Codec.getCodec(DataType.map(CUSTOM_FOO, text()));
        Assert.assertNotNull(mapType);
        Assert.assertEquals(mapType.nameComparator().getClass(), BytesType.class);
        Assert.assertEquals(mapType.valueComparator().getClass(), UTF8Type.class);
    }

    @Test(groups = "unit")
    public void testCustomValueMap() throws Exception {
        MapType<?, ?> mapType = (MapType) Codec.getCodec(DataType.map(text(), CUSTOM_FOO));
        Assert.assertNotNull(mapType);
        Assert.assertEquals(mapType.nameComparator().getClass(), UTF8Type.class);
        Assert.assertEquals(mapType.valueComparator().getClass(), BytesType.class);
    }

    @Test(groups = "unit")
    public void testSetTypeCodec() throws Exception {
        Map<Name, SetType<?>> dateTypeSetTypeMap = new HashMap<Name, SetType<?>>(Codec.buildSets());

        final Set<DataType> dataTypes = new HashSet<DataType>(allPrimitiveTypes());

        // Special case: DataType varchar yields Codec.rawTypeToDataType(...) => text causing assertion fail
        // Solution: Remove DataType.varchar and check that it yields DataType.text
        Assert.assertTrue(dataTypes.remove(varchar()));
        Assert.assertEquals(Codec.rawTypeToDataType(dateTypeSetTypeMap.get(varchar().getName()).nameComparator()), text()) ;
        dateTypeSetTypeMap.remove(varchar().getName());

        for (DataType dataType : dataTypes) {
            final SetType<?> setType = dateTypeSetTypeMap.remove(dataType.getName());
            Assert.assertEquals(Codec.rawTypeToDataType(setType.nameComparator()),dataType) ;
        }
        Assert.assertTrue(dateTypeSetTypeMap.isEmpty(), "All map types should have been tested: "+dateTypeSetTypeMap);
    }

    @Test(groups = "unit")
    public void testListTypeCodec() throws Exception {
        Map<Name,ListType<?>> dateTypeListetTypeMap = new HashMap<Name, ListType<?>>(Codec.buildLists());

        final Set<DataType> dataTypes = new HashSet<DataType>(allPrimitiveTypes());

        // Special case: DataType varchar yields Codec.rawTypeToDataType(...) => text causing assertion fail
        // Solution: Remove DataType.varchar and check that it yields DataType.text
        Assert.assertTrue(dataTypes.remove(varchar()));
        final AbstractType<?> rawType = dateTypeListetTypeMap.get(varchar().getName()).valueComparator();
        Assert.assertEquals(Codec.rawTypeToDataType(rawType), text()) ;
        dateTypeListetTypeMap.remove(varchar().getName());

        for (DataType dataType : dataTypes) {
            final ListType<?> setType = dateTypeListetTypeMap.remove(dataType.getName());
            Assert.assertEquals(Codec.rawTypeToDataType(setType.valueComparator()),dataType) ;
        }
        Assert.assertTrue(dateTypeListetTypeMap.isEmpty(), "All set types should have been tested: "+dateTypeListetTypeMap);
    }

    @Test(groups = "unit")
    public void testMapTypeCodec() throws Exception {
        final Map<Name, Map<Name, MapType<?, ?>>> dateTypeMapTypeMap = Maps.newHashMap(Codec.buildMaps());

        for (DataType dataTypeArg0 : allPrimitiveTypes()) {
            final Map<Name, MapType<?, ?>> innerNameToMapType = Maps.newHashMap(dateTypeMapTypeMap.get(dataTypeArg0.getName()));

            for (DataType dataTypeArg1 : allPrimitiveTypes()) {
                final MapType<?, ?> mapType = innerNameToMapType.remove(dataTypeArg1.getName());
                testPairAgainstMapType(dataTypeArg0, dataTypeArg1, mapType);
            }
            Assert.assertTrue(innerNameToMapType.isEmpty(),
                    "All inner map types should have been tested for outer type '"+dataTypeArg0+"': " + dateTypeMapTypeMap);
            dateTypeMapTypeMap.remove(dataTypeArg0.getName());
        }
        Assert.assertTrue(dateTypeMapTypeMap.isEmpty(), "All map types should have been tested: "+dateTypeMapTypeMap);
    }

    private void testPairAgainstMapType(DataType dataTypeArg0, DataType dataTypeArg1, MapType<?, ?> mapType) {
        // Special case: DataType varchar yields Codec.rawTypeToDataType(...) => text causing assertion fail
        // Solution: Remove DataType.varchar and check that it yields DataType.text

        // In the map case this would be quite many combinations of text/varchar so I translate varchar to text
        // in order to expect text instead of varchar back from those cases:
        if (dataTypeArg0.equals(varchar())) dataTypeArg0 = text();
        if (dataTypeArg1.equals(varchar())) dataTypeArg1 = text();

        Assert.assertEquals(Codec.rawTypeToDataType(mapType.nameComparator()), dataTypeArg0) ;
        Assert.assertEquals(Codec.rawTypeToDataType(mapType.valueComparator()),dataTypeArg1) ;
    }
}
