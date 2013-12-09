package com.datastax.driver.core;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.utils.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;

import static com.datastax.driver.core.DataType.*;

/**
 * User: Olve S. Hansen mailto:olve@vimond.com
 * Date: 06/12/13
 * Time: 23:37
 */
public class CodecTest {

    @Test(groups = "unit")
    public void testSetTypeCodec() throws Exception {
        Map<DataType, SetType<?>> dateTypeSetTypeMap = new HashMap<DataType, SetType<?>>(Codec.buildSets());

        final Set<DataType> dataTypes = new HashSet<DataType>(allPrimitiveTypes());

        //Special case: DataType varchar yields Codec.rawTypeToDataType(...) => text causing assertion fail
        //Solution: Remove DataType.varchar and check that it yields DataType.text
        Assert.assertTrue(dataTypes.remove(varchar()));
        Assert.assertEquals(Codec.rawTypeToDataType(dateTypeSetTypeMap.get(varchar()).nameComparator()), text()) ;
        dateTypeSetTypeMap.remove(varchar());

        for (DataType dataType : dataTypes) {
            final SetType<?> setType = dateTypeSetTypeMap.remove(dataType);
            Assert.assertEquals(Codec.rawTypeToDataType(setType.nameComparator()),dataType) ;
        }
        Assert.assertTrue(dateTypeSetTypeMap.isEmpty(), "All map types should have been tested: "+dateTypeSetTypeMap);

    }

    @Test(groups = "unit")
    public void testListTypeCodec() throws Exception {
        Map<DataType,ListType<?>> dateTypeListetTypeMap = new HashMap<DataType, ListType<?>>(Codec.buildLists());

        final Set<DataType> dataTypes = new HashSet<DataType>(allPrimitiveTypes());

        //Special case: DataType varchar yields Codec.rawTypeToDataType(...) => text causing assertion fail
        //Solution: Remove DataType.varchar and check that it yields DataType.text
        Assert.assertTrue(dataTypes.remove(varchar()));
        final AbstractType<?> rawType = dateTypeListetTypeMap.get(varchar()).valueComparator();
        Assert.assertEquals(Codec.rawTypeToDataType(rawType), text()) ;
        dateTypeListetTypeMap.remove(varchar());


        for (DataType dataType : dataTypes) {
            final ListType<?> setType = dateTypeListetTypeMap.remove(dataType);
            Assert.assertEquals(Codec.rawTypeToDataType(setType.valueComparator()),dataType) ;
        }
        Assert.assertTrue(dateTypeListetTypeMap.isEmpty(), "All set types should have been tested: "+dateTypeListetTypeMap);

    }

    @Test(groups = "unit")
    public void testMapTypeCodec() throws Exception {
        Map<Pair<DataType,DataType>,MapType<?,?>> dateTypeSetTypeMap =
                new HashMap<Pair<DataType, DataType>, MapType<?, ?>>(Codec.buildMaps());

        for (DataType dataTypeArg1 : allPrimitiveTypes()) {
            for (DataType dataTypeArg2 : allPrimitiveTypes()) {
                final Pair<DataType, DataType> pair = Pair.create(dataTypeArg1, dataTypeArg2);
                final MapType<?, ?> mapType = dateTypeSetTypeMap.remove(pair);

                testPairAgainstMapType(pair, mapType);
            }
        }
        Assert.assertTrue(dateTypeSetTypeMap.isEmpty(), "All map types should have been tested: "+dateTypeSetTypeMap);
    }


    private void testPairAgainstMapType(Pair<DataType, DataType> pair, MapType<?, ?> mapType) {
        System.out.println("testing pair: "+pair);
        DataType dataType = pair.left;
        DataType dataType1 = pair.right;

        //Special case: DataType varchar yields Codec.rawTypeToDataType(...) => text causing assertion fail
        //Solution: Remove DataType.varchar and check that it yields DataType.text

        //In the map case this would be quite many combinations of text/varchar so I translate varchar to text
        //in order to expect text instead of varchar back from those cases:
        if(dataType.equals(varchar())) dataType = text();
        if(dataType1.equals(varchar())) dataType1 = text();

        Assert.assertEquals(Codec.rawTypeToDataType(mapType.nameComparator()), dataType) ;
        Assert.assertEquals(Codec.rawTypeToDataType(mapType.valueComparator()),dataType1) ;
    }


    public void testRawTypeToDataType() throws Exception {

    }
}
