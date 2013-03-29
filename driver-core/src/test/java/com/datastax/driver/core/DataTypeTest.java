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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

/**
 * Tests DataType class to ensure data sent in is the same as data received
 * All tests are executed via a Simple Statement
 * Counters are the only datatype not tested within the entirety of the suite.
 *     There is, however, an isolated test case that needs to be implemented.
 * All statements and sample data is easily exportable via the print_*() methods.
 */
public class DataTypeTest extends CCMBridge.PerClassSingleNodeCluster {

    private final static Set<DataType> DATA_TYPE_PRIMITIVES = DataType.allPrimitiveTypes();
    private final static Set<DataType.Name> DATA_TYPE_NON_PRIMITIVE_NAMES = EnumSet.of(DataType.Name.MAP, DataType.Name.SET, DataType.Name.LIST);

    private final static String PRIMITIVE_INSERT_FORMAT = "INSERT INTO %1$s (k, v) VALUES (%2$s, %2$s);";
    private final static String BASIC_SELECT_FORMAT = "SELECT k, v FROM %1$s;";

    private final static String COLLECTION_INSERT_FORMAT = "INSERT INTO %1$s (k, v) VALUES (%2$s, %3$s);";
    private final static String MAP_INSERT_FORMAT = "INSERT INTO %1$s (k, v) VALUES (%3$s, {%2$s: %3$s});";

    private final static HashMap<DataType, Object> SAMPLE_DATA = getSampleData();
    private final static HashMap<DataType, Object> SAMPLE_COLLECTIONS = getSampleCollections();

    private final static Collection<String> PRIMITIVE_INSERT_STATEMENTS = getPrimitiveInsertStatements();
    private final static HashMap<DataType, String> PRIMITIVE_SELECT_STATEMENTS = getPrimitiveSelectStatements();

    private final static Collection<String> COLLECTION_INSERT_STATEMENTS = getCollectionInsertStatements();
    private final static HashMap<DataType, String> COLLECTION_SELECT_STATEMENTS = getCollectionSelectStatements();


    private static boolean exclude(DataType t) {
        return t.getName() == DataType.Name.COUNTER;
    }

    /**
     * Generates the table definitions that will be used in testing
     */
    protected Collection<String> getTableDefinitions() {
        ArrayList<String> tableDefinitions = new ArrayList<String>();

        // Create primitive data type definitions
        for (DataType dataType : DATA_TYPE_PRIMITIVES) {
            if (exclude(dataType))
                continue;

            tableDefinitions.add(String.format("CREATE TABLE %1$s (k %2$s PRIMARY KEY, v %1$s)", dataType, dataType));
        }

        // Create collection data type definitions
        for (DataType.Name dataTypeName : DATA_TYPE_NON_PRIMITIVE_NAMES) {
            // Create MAP data type definitions
            if (dataTypeName == DataType.Name.MAP) {
                for (DataType typeArgument1 : DATA_TYPE_PRIMITIVES) {
                    if (exclude(typeArgument1))
                        continue;

                    for (DataType typeArgument2 : DATA_TYPE_PRIMITIVES) {
                        if (exclude(typeArgument2))
                            continue;

                        tableDefinitions.add(String.format("CREATE TABLE %1$s_%2$s_%3$s (k %3$s PRIMARY KEY, v %1$s<%2$s, %3$s>)", dataTypeName, typeArgument1, typeArgument2));
                    }
                }
            // Create SET and LIST data type definitions
            } else {
                for (DataType typeArgument : DATA_TYPE_PRIMITIVES) {
                    if (exclude(typeArgument))
                        continue;

                    tableDefinitions.add(String.format("CREATE TABLE %1$s_%2$s (k %2$s PRIMARY KEY, v %1$s<%2$s>)", dataTypeName, typeArgument));
                }
            }
        }

        return tableDefinitions;
    }

    /**
     * Generates the sample data that will be used in testing
     */
    private static HashMap<DataType, Object> getSampleData() {
        HashMap<DataType, Object> sampleData = new HashMap<DataType, Object>();

        for (DataType dataType : DATA_TYPE_PRIMITIVES) {
            switch (dataType.getName()) {
                case ASCII:
                    sampleData.put(dataType, new String("ascii"));
                    break;
                case BIGINT:
                    sampleData.put(dataType, Long.MAX_VALUE);
                    break;
                case BLOB:
                    ByteBuffer bb = ByteBuffer.allocate(58);
                    bb.putShort((short) 0xCAFE);
                    bb.flip();
                    sampleData.put(dataType, bb);
                    break;
                case BOOLEAN:
                    sampleData.put(dataType, Boolean.TRUE);
                    break;
                case COUNTER:
                    // Not supported in an insert statement
                    break;
                case DECIMAL:
                    sampleData.put(dataType, new BigDecimal("12.3E+7"));
                    break;
                case DOUBLE:
                    sampleData.put(dataType, Double.MAX_VALUE);
                    break;
                case FLOAT:
                    sampleData.put(dataType, Float.MAX_VALUE);
                    break;
                case INET:
                    try {
                        sampleData.put(dataType, InetAddress.getByName("123.123.123.123"));
                    } catch (java.net.UnknownHostException e) {}
                    break;
                case INT:
                    sampleData.put(dataType, Integer.MAX_VALUE);
                    break;
                case TEXT:
                    sampleData.put(dataType, new String("text"));
                    break;
                case TIMESTAMP:
                    sampleData.put(dataType, new Date(872835240000L));
                    break;
                case TIMEUUID:
                    sampleData.put(dataType, UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66"));
                    break;
                case UUID:
                    sampleData.put(dataType, UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff00"));
                    break;
                case VARCHAR:
                    sampleData.put(dataType, new String("varchar"));
                    break;
                case VARINT:
                    sampleData.put(dataType, new BigInteger(Integer.toString(Integer.MAX_VALUE) + "000"));
                    break;
                default:
                    throw new RuntimeException("Missing handling of " + dataType);
            }
        }

        return sampleData;
    }

    /**
     * Generates the sample collections that will be used in testing
     */ private static HashMap<DataType, Object> getSampleCollections() {
        HashMap<DataType, Object> sampleCollections = new HashMap<DataType, Object>();
        HashMap<DataType, Object> setAndListCollection;
        HashMap<DataType, HashMap<DataType, Object>> mapCollection;

        for (DataType.Name dataTypeName : DATA_TYPE_NON_PRIMITIVE_NAMES) {
            switch (dataTypeName) {
                case LIST:
                    for (DataType typeArgument : DATA_TYPE_PRIMITIVES) {
                        if (exclude(typeArgument))
                            continue;

                        List list = new ArrayList();
                        for (int i = 0; i < 5; i++) {
                            list.add(SAMPLE_DATA.get(typeArgument));
                        }

                        setAndListCollection = new HashMap<DataType, Object>();
                        setAndListCollection.put(typeArgument, list);
                        sampleCollections.put(DataType.list(typeArgument), setAndListCollection);
                    }
                    break;
                case SET:
                    for (DataType typeArgument : DATA_TYPE_PRIMITIVES) {
                        if (exclude(typeArgument))
                            continue;

                        Set set = new HashSet();
                        for (int i = 0; i < 5; i++) {
                            set.add(SAMPLE_DATA.get(typeArgument));
                        }

                        setAndListCollection = new HashMap<DataType, Object>();
                        setAndListCollection.put(typeArgument, set);
                        sampleCollections.put(DataType.set(typeArgument), setAndListCollection);
                    }
                    break;
                case MAP:
                    for (DataType typeArgument1 : DATA_TYPE_PRIMITIVES) {
                        if (exclude(typeArgument1))
                            continue;

                        for (DataType typeArgument2 : DATA_TYPE_PRIMITIVES) {
                            if (exclude(typeArgument2))
                                continue;

                            HashMap<DataType, Object> map = new HashMap<DataType, Object>();
                            map.put(typeArgument1, SAMPLE_DATA.get(typeArgument2));

                            mapCollection = new HashMap<DataType, HashMap<DataType, Object>>();
                            mapCollection.put(typeArgument1, map);
                            sampleCollections.put(DataType.map(typeArgument1, typeArgument2), mapCollection);
                        }
                    }
                    break;
                default:
                    throw new RuntimeException("Missing handling of " + dataTypeName);
            }
        }

        return sampleCollections;
    }

    /**
     * Helper method to stringify SAMPLE_DATA for simple insert statements
     */
    private static String helperStringifiedData(DataType dataType) {
        String value = SAMPLE_DATA.get(dataType).toString();

        switch (dataType.getName()) {
            case BLOB:
                value = "0xCAFE";
                break;

            case INET:
                InetAddress v1 = (InetAddress) SAMPLE_DATA.get(dataType);
                value = String.format("'%s'", v1.getHostAddress());
                break;

            case TIMESTAMP:
                Date v2 = (Date) SAMPLE_DATA.get(dataType);
                value = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(v2);
            case ASCII:
            case TEXT:
            case VARCHAR:
                value = String.format("'%s'", value);
                break;

            default:
                break;
        }

        return value;
    }

    /**
     * Generates the insert statements that will be used in testing
     */
    private static Collection<String> getPrimitiveInsertStatements() {
        ArrayList<String> insertStatements = new ArrayList<String>();

        for (DataType dataType : SAMPLE_DATA.keySet()) {
            String value = helperStringifiedData(dataType);
            insertStatements.add(String.format(PRIMITIVE_INSERT_FORMAT, dataType, value));
        }

        return insertStatements;
    }

    /**
     * Generates the select statements that will be used in testing
     */
    private static HashMap<DataType, String> getPrimitiveSelectStatements() {
        HashMap<DataType, String> selectStatements = new HashMap<DataType, String>();

        for (DataType dataType : SAMPLE_DATA.keySet()) {
            selectStatements.put(dataType, String.format(BASIC_SELECT_FORMAT, dataType));
        }

        return selectStatements;
    }

    /**
     * Helper method to generate table names in the form of:
     * DataType_TypeArgument[_TypeArgument]
     */
    private static String helperGenerateTableName(DataType dataType) {
        String tableName = dataType.getName().toString();
        for (DataType typeArgument : dataType.getTypeArguments())
            tableName += "_" + typeArgument;

        return tableName;
    }

    /**
     * Generates the insert statements that will be used in testing
     */
    private static Collection<String> getCollectionInsertStatements() {
        ArrayList<String> insertStatements = new ArrayList<String>();

        String tableName;
        String key;
        String value;
        for (DataType dataType : SAMPLE_COLLECTIONS.keySet()) {
            HashMap<DataType, Object> sampleValueMap = (HashMap<DataType, Object>) SAMPLE_COLLECTIONS.get(dataType);

            // Create tableName in form of: DataType_TypeArgument[_TypeArgument]
            tableName = helperGenerateTableName(dataType);

            if (dataType.getName() == DataType.Name.MAP) {
                List<DataType> typeArgument = dataType.getTypeArguments();

                key = helperStringifiedData(typeArgument.get(0));
                value = helperStringifiedData(typeArgument.get(1));

                insertStatements.add(String.format(MAP_INSERT_FORMAT, tableName, key, value));
            } else if (dataType.getName() == DataType.Name.LIST) {
                DataType typeArgument = sampleValueMap.keySet().iterator().next();
                key = helperStringifiedData(typeArgument);

                // Create the value to be a list of the same 5 elements
                value = "[";
                for (int i = 0; i < 5; i++)
                    value += key + ",";
                value = value.substring(0, value.length() - 1) + "]";

                insertStatements.add(String.format(COLLECTION_INSERT_FORMAT, tableName, key, value));
            } else {
                DataType typeArgument = sampleValueMap.keySet().iterator().next();
                key = helperStringifiedData(typeArgument);
                value = "{" + key + "}";

                insertStatements.add(String.format(COLLECTION_INSERT_FORMAT, tableName, key, value));
            }
        }

        return insertStatements;
    }

    /**
     * Generates the select statements that will be used in testing
     */
    private static HashMap<DataType, String> getCollectionSelectStatements() {
        HashMap<DataType, String> selectStatements = new HashMap<DataType, String>();

        String tableName;
        for (DataType dataType : SAMPLE_COLLECTIONS.keySet()) {
            tableName = helperGenerateTableName(dataType);
            selectStatements.put(dataType, String.format(BASIC_SELECT_FORMAT, tableName));
        }

        return selectStatements;
    }






    /**
     * Test simple statement inserts for all primitive data types
     */
    public void primitiveInsertTest() throws Throwable {
        ResultSet rs;
        for (String execute_string : PRIMITIVE_INSERT_STATEMENTS) {
            rs = session.execute(execute_string);
            assertTrue(rs.isExhausted());
        }
        assertEquals(SAMPLE_DATA.size(), 15);
        assertEquals(PRIMITIVE_INSERT_STATEMENTS.size(), SAMPLE_DATA.size());
    }

    /**
     * Validate simple statement selects for all primitive data types
     */
    public void primitiveSelectTest() throws Throwable {
        String execute_string;
        Object value;
        Row row;
        for (DataType dataType : PRIMITIVE_SELECT_STATEMENTS.keySet()) {
            execute_string = PRIMITIVE_SELECT_STATEMENTS.get(dataType);
            row = session.execute(execute_string).one();

            value = SAMPLE_DATA.get(dataType);
            assertEquals(TestUtils.getValue(row, "k", dataType), value);
            assertEquals(TestUtils.getValue(row, "v", dataType), value);
        }
        assertEquals(SAMPLE_DATA.size(), 15);
        assertEquals(PRIMITIVE_SELECT_STATEMENTS.keySet().size(), SAMPLE_DATA.size());
    }

    /**
     * Test simple statement inserts and selects for all primitive data types
     */
    @Test(groups = "integration")
    public void primitiveTests() throws Throwable {
        primitiveInsertTest();
        primitiveSelectTest();
    }

    /**
     * Test simple statement inserts for all collection data types
     */
    public void collectionInsertTest() throws Throwable {
        ResultSet rs;
        for (String execute_string : COLLECTION_INSERT_STATEMENTS) {
            rs = session.execute(execute_string);
            assertTrue(rs.isExhausted());
        }
        assertEquals(SAMPLE_COLLECTIONS.size(), 255);
        assertEquals(COLLECTION_INSERT_STATEMENTS.size(), SAMPLE_COLLECTIONS.size());
    }

    /**
     * Test simple statement selects for all collection data types
     */
    public void collectionSelectTest() throws Throwable {
        HashMap<DataType, Object> sampleValueMap;
        String execute_string;
        DataType typeArgument1;
        DataType typeArgument2;
        Row row;
        for (DataType dataType : COLLECTION_SELECT_STATEMENTS.keySet()) {
            execute_string = COLLECTION_SELECT_STATEMENTS.get(dataType);
            row = session.execute(execute_string).one();

            sampleValueMap = (HashMap<DataType, Object>) SAMPLE_COLLECTIONS.get(dataType);
            typeArgument1 = dataType.getTypeArguments().get(0);
            if (dataType.getName() == DataType.Name.MAP) {
                typeArgument2 = dataType.getTypeArguments().get(1);

                // Create a copy of the map that is being expected
                HashMap<DataType, Object> sampleMap = (HashMap<DataType, Object>) sampleValueMap.get(typeArgument1);
                Object mapKey = SAMPLE_DATA.get(sampleMap.keySet().iterator().next());
                Object mapValue = sampleMap.values().iterator().next();
                HashMap expectedMap = new HashMap();
                expectedMap.put(mapKey, mapValue);

                assertEquals(TestUtils.getValue(row, "k", typeArgument2), SAMPLE_DATA.get(typeArgument2));
                assertEquals(TestUtils.getValue(row, "v", dataType), expectedMap);
            } else {
                Object expectedValue = sampleValueMap.get(typeArgument1);

                assertEquals(TestUtils.getValue(row, "k", typeArgument1), SAMPLE_DATA.get(typeArgument1));
                assertEquals(TestUtils.getValue(row, "v", dataType), expectedValue);
            }
        }
        assertEquals(SAMPLE_COLLECTIONS.size(), 255);
        assertEquals(COLLECTION_SELECT_STATEMENTS.keySet().size(), SAMPLE_COLLECTIONS.size());
    }

    /**
     * Test simple statement inserts and selects for all collection data types
     */
    @Test(groups = "integration")
    public void collectionTest() throws Throwable {
        collectionInsertTest();
        collectionSelectTest();
    }

    /**
     * Test TTLs.
     */
    // TODO: @Test(groups = "integration")
    public void ttlTest() throws Throwable {

    }

    /**
     * Test Counters in an isolated format.
     */
    // TODO: @Test(groups = "integration")
    public void countersTest() throws Throwable {

    }

    /**
     * Test tombstones.
     */
    // TODO: @Test(groups = "integration")
    public void tombstonesTest() throws Throwable {

    }






    /**
     * Prints the table definitions that will be used in testing
     * (for exporting purposes)
     */
    @Test(groups = { "docs" })
    public void printTableDefinitions() {
        // Prints the full list of table definitions
        for (String definition : getTableDefinitions()) {
            System.out.println(definition);
        }
    }

    /**
     * Prints the sample data that will be used in testing
     * (for exporting purposes)
     */
    @Test(groups = { "docs" })
    public void printSampleData() {
        for (DataType dataType : SAMPLE_DATA.keySet()) {
            Object sampleValue = SAMPLE_DATA.get(dataType);
            System.out.println(String.format("%1$-10s %2$s", dataType, sampleValue));
        }
    }

    /**
     * Prints the sample collections that will be used in testing
     * (for exporting purposes)
     */
    @Test(groups = { "docs" })
    public void printSampleCollections() {
        for (DataType dataType : SAMPLE_COLLECTIONS.keySet()) {
            HashMap<DataType, Object> sampleValueMap = (HashMap<DataType, Object>) SAMPLE_COLLECTIONS.get(dataType);

            if (dataType.getName() == DataType.Name.MAP) {
                DataType typeArgument = sampleValueMap.keySet().iterator().next();
                HashMap<DataType, Object> sampleMap = (HashMap<DataType, Object>) sampleValueMap.get(typeArgument);

                Object mapKey = SAMPLE_DATA.get(typeArgument);
                Object mapValue = sampleMap.get(typeArgument);
                System.out.println(String.format("%1$-30s {%2$s : %3$s}", dataType, mapKey, mapValue));
            } else {
                DataType typeArgument = sampleValueMap.keySet().iterator().next();
                Object sampleValue = sampleValueMap.get(typeArgument);

                System.out.println(String.format("%1$-30s %2$s", dataType, sampleValue));
            }
        }
    }

    /**
     * Prints the simple insert statements that will be used in testing
     * (for exporting purposes)
     */
    @Test(groups = { "docs" })
    public void printPrimitiveInsertStatements() {
        for (String execute_string : PRIMITIVE_INSERT_STATEMENTS) {
            System.out.println(execute_string);
        }
    }

    /**
     * Prints the simple select statements that will be used in testing
     * (for exporting purposes)
     */
    @Test(groups = { "docs" })
    public void printPrimitiveSelectStatements() {
        for (String execute_string : PRIMITIVE_SELECT_STATEMENTS.values()) {
            System.out.println(execute_string);
        }
    }

    /**
     * Prints the simple insert statements that will be used in testing
     * (for exporting purposes)
     */
    @Test(groups = { "docs" })
    public void printCollectionInsertStatements() {
        for (String execute_string : COLLECTION_INSERT_STATEMENTS) {
            System.out.println(execute_string);
        }
    }

    /**
     * Prints the simple insert statements that will be used in testing
     * (for exporting purposes)
     */
    @Test(groups = { "docs" })
    public void printCollectionSelectStatements() {
        for (String execute_string : COLLECTION_SELECT_STATEMENTS.values()) {
            System.out.println(execute_string);
        }
    }
}
