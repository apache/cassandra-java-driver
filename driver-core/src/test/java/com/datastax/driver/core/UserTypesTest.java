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

import java.lang.Exception;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.ImmutableSet;
import com.google.common.base.Joiner;

import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

import static com.datastax.driver.core.Metadata.quote;
import static com.datastax.driver.core.TestUtils.versionCheck;
import static org.testng.Assert.assertNotEquals;

public class UserTypesTest extends CCMBridge.PerClassSingleNodeCluster {

    private final static List<DataType> DATA_TYPE_PRIMITIVES = new ArrayList<DataType>(DataType.allPrimitiveTypes());
    private final static List<DataType.Name> DATA_TYPE_NON_PRIMITIVE_NAMES =
            new ArrayList<DataType.Name>(EnumSet.of(DataType.Name.LIST, DataType.Name.SET, DataType.Name.MAP, DataType.Name.TUPLE));

    private final static HashMap<DataType, Object> SAMPLE_DATA = DataTypeIntegrationTest.getSampleData();

    @Override
    protected Collection<String> getTableDefinitions() {
        versionCheck(2.1, 0, "This will only work with Cassandra 2.1.0");

        String type1 = "CREATE TYPE phone (alias text, number text)";
        String type2 = "CREATE TYPE address (street text, \"ZIP\" int, phones set<phone>)";

        String table = "CREATE TABLE user (id int PRIMARY KEY, addr address)";

        return Arrays.asList(type1, type2, table);
    }

    /**
     * Basic write read test to ensure UDTs are stored and retrieved correctly.
     *
     * @throws Exception
     */
    @Test(groups = "short")
    public void simpleWriteReadTest() throws Exception {
        int userId = 0;

        try {
            session.execute("USE ks");
            PreparedStatement ins = session.prepare("INSERT INTO user(id, addr) VALUES (?, ?)");
            PreparedStatement sel = session.prepare("SELECT * FROM user WHERE id=?");

            UserType addrDef = cluster.getMetadata().getKeyspace("ks").getUserType("address");
            UserType phoneDef = cluster.getMetadata().getKeyspace("ks").getUserType("phone");

            UDTValue phone1 = phoneDef.newValue().setString("alias", "home").setString("number", "0123548790");
            UDTValue phone2 = phoneDef.newValue().setString("alias", "work").setString("number", "0698265251");

            UDTValue addr = addrDef.newValue().setString("street", "1600 Pennsylvania Ave NW").setInt(quote("ZIP"), 20500).setSet("phones", ImmutableSet.of(phone1, phone2));

            session.execute(ins.bind(userId, addr));

            Row r = session.execute(sel.bind(userId)).one();

            assertEquals(r.getInt("id"), 0);
            assertEquals(r.getUDTValue("addr"), addr);
        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }

    /**
     * Run simpleWriteReadTest with unprepared requests.
     * @throws Exception
     */
    @Test(groups = "short")
    public void simpleUnpreparedWriteReadTest() throws Exception {
        int userId = 1;

        try {
            session.execute("USE ks");
            UserType addrDef = cluster.getMetadata().getKeyspace("ks").getUserType("address");
            UserType phoneDef = cluster.getMetadata().getKeyspace("ks").getUserType("phone");

            UDTValue phone1 = phoneDef.newValue().setString("alias", "home").setString("number", "0123548790");
            UDTValue phone2 = phoneDef.newValue().setString("alias", "work").setString("number", "0698265251");

            UDTValue addr = addrDef.newValue().setString("street", "1600 Pennsylvania Ave NW").setInt(quote("ZIP"), 20500).setSet("phones", ImmutableSet.of(phone1, phone2));

            session.execute("INSERT INTO user(id, addr) VALUES (?, ?)", userId, addr);

            Row r = session.execute("SELECT * FROM user WHERE id=?", userId).one();

            assertEquals(r.getInt("id"), 1);
            assertEquals(r.getUDTValue("addr"), addr);
        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }

    /**
     * Run simpleWriteReadTest with unprepared requests.
     * @throws Exception
     */
    @Test(groups = "short")
    public void nonExistingTypesTest() throws Exception {
        try {
            session.execute("USE ks");

            UserType addrDef = cluster.getMetadata().getKeyspace("ks").getUserType("address1");
            UserType phoneDef = cluster.getMetadata().getKeyspace("ks").getUserType("phone1");
            assertEquals(addrDef, null);
            assertEquals(phoneDef, null);

            addrDef = cluster.getMetadata().getKeyspace("ks").getUserType("address");
            phoneDef = cluster.getMetadata().getKeyspace("ks").getUserType("phone");
            assertNotEquals(addrDef, null);
            assertNotEquals(phoneDef, null);

            // create keyspace
            session.execute("CREATE KEYSPACE nonExistingTypesTest " +
                    "WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}");
            session.execute("USE nonExistingTypesTest");

            addrDef = cluster.getMetadata().getKeyspace("nonExistingTypesTest").getUserType("address");
            phoneDef = cluster.getMetadata().getKeyspace("nonExistingTypesTest").getUserType("phone");
            assertEquals(addrDef, null);
            assertEquals(phoneDef, null);

            session.execute("USE ks");

            addrDef = cluster.getMetadata().getKeyspace("ks").getUserType("address");
            phoneDef = cluster.getMetadata().getKeyspace("ks").getUserType("phone");
            assertNotEquals(addrDef, null);
            assertNotEquals(phoneDef, null);

        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }

    /**
     * Test for ensuring extra-lengthy udts are handled correctly.
     * Original code found in python-driver:integration.standard.test_udts.py:test_udt_sizes
     * @throws Exception
     */
    @Test(groups = "short")
    public void udtSizesTest() throws Exception {
        int MAX_TEST_LENGTH = 1024;

        try {
            // create keyspace
            session.execute("CREATE KEYSPACE test_udt_sizes " +
                    "WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}");
            session.execute("USE test_udt_sizes");

            // create the seed udt
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < MAX_TEST_LENGTH; ++i) {
                sb.append(String.format("v_%s int", i));

                if (i + 1 < MAX_TEST_LENGTH)
                    sb.append(",");
            }
            session.execute(String.format("CREATE TYPE lengthy_udt (%s)", sb.toString()));

            // create a table with multiple sizes of udts
            session.execute("CREATE TABLE mytable (k int PRIMARY KEY, v lengthy_udt)");

            // hold onto the UserType for future use
            UserType udtDef = cluster.getMetadata().getKeyspace("test_udt_sizes").getUserType("lengthy_udt");

            // verify inserts and reads
            for (int i : Arrays.asList(0, 1, 2, 3, MAX_TEST_LENGTH)) {
                // create udt
                UDTValue createdUDT = udtDef.newValue();
                for (int j = 0; j < i; ++j) {
                    createdUDT.setInt(j, j);
                }

                // write udt
                session.execute("INSERT INTO mytable (k, v) VALUES (0, ?)", createdUDT);

                // verify udt was written and read correctly
                UDTValue r = session.execute("SELECT v FROM mytable WHERE k=0")
                        .one().getUDTValue("v");
                assertEquals(r.toString(), createdUDT.toString());
            }
        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }

    /**
     * Test for inserting various types of DATA_TYPE_PRIMITIVES into UDT's.
     * Original code found in python-driver:integration.standard.test_udts.py:test_primitive_datatypes
     * @throws Exception
     */
    @Test(groups = "short")
    public void testPrimitiveDatatypes() throws Exception {
        try {
            // create keyspace
            session.execute("CREATE KEYSPACE testPrimitiveDatatypes " +
                    "WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}");
            session.execute("USE testPrimitiveDatatypes");

            // create UDT
            List<String> alpha_type_list = new ArrayList<String>();
            int startIndex = (int) 'a';
            for (int i = 0; i < DATA_TYPE_PRIMITIVES.size(); i++) {
                alpha_type_list.add(String.format("%s %s", Character.toString((char) (startIndex + i)),
                        DATA_TYPE_PRIMITIVES.get(i).getName()));
            }

            session.execute(String.format("CREATE TYPE alldatatypes (%s)", Joiner.on(',').join(alpha_type_list)));
            session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b alldatatypes)");

            // insert UDT data
            UserType alldatatypesDef = cluster.getMetadata().getKeyspace("testPrimitiveDatatypes").getUserType("alldatatypes");
            UDTValue alldatatypes = alldatatypesDef.newValue();

            for (int i = 0; i < DATA_TYPE_PRIMITIVES.size(); i++) {
                DataType dataType = DATA_TYPE_PRIMITIVES.get(i);
                String index = Character.toString((char) (startIndex + i));
                Object sampleData = SAMPLE_DATA.get(dataType);

                switch (dataType.getName()) {
                    case ASCII:
                        alldatatypes.setString(index, (String) sampleData);
                        break;
                    case BIGINT:
                        alldatatypes.setLong(index, ((Long) sampleData).longValue());
                        break;
                    case BLOB:
                        alldatatypes.setBytes(index, (ByteBuffer) sampleData);
                        break;
                    case BOOLEAN:
                        alldatatypes.setBool(index, ((Boolean) sampleData).booleanValue());
                        break;
                    case DECIMAL:
                        alldatatypes.setDecimal(index, (BigDecimal) sampleData);
                        break;
                    case DOUBLE:
                        alldatatypes.setDouble(index, ((Double) sampleData).doubleValue());
                        break;
                    case FLOAT:
                        alldatatypes.setFloat(index, ((Float) sampleData).floatValue());
                        break;
                    case INET:
                        alldatatypes.setInet(index, (InetAddress) sampleData);
                        break;
                    case INT:
                        alldatatypes.setInt(index, ((Integer) sampleData).intValue());
                        break;
                    case TEXT:
                        alldatatypes.setString(index, (String) sampleData);
                        break;
                    case TIMESTAMP:
                        alldatatypes.setDate(index, ((Date) sampleData));
                        break;
                    case TIMEUUID:
                        alldatatypes.setUUID(index, (UUID) sampleData);
                        break;
                    case UUID:
                        alldatatypes.setUUID(index, (UUID) sampleData);
                        break;
                    case VARCHAR:
                        alldatatypes.setString(index, (String) sampleData);
                        break;
                    case VARINT:
                        alldatatypes.setVarint(index, (BigInteger) sampleData);
                        break;
                }
            }

            PreparedStatement ins = session.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)");
            session.execute(ins.bind(0, alldatatypes));

            // retrieve and verify data
            ResultSet rs = session.execute("SELECT * FROM mytable");
            List<Row> rows = rs.all();
            assertEquals(1, rows.size());

            Row row = rows.get(0);

            assertEquals(row.getInt("a"), 0);
            assertEquals(row.getUDTValue("b"), alldatatypes);

        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }

    /**
     * Test for inserting various types of DATA_TYPE_NON_PRIMITIVE into UDT's
     * Original code found in python-driver:integration.standard.test_udts.py:test_nonprimitive_datatypes
     * @throws Exception
     */
    @Test(groups = "short")
    public void testNonPrimitiveDatatypes() throws Exception {
        try {
            // create keyspace
            session.execute("CREATE KEYSPACE test_nonprimitive_datatypes " +
                    "WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}");
            session.execute("USE test_nonprimitive_datatypes");

            // counters are not allowed inside collections
            DATA_TYPE_PRIMITIVES.remove(DataType.counter());

            // create UDT
            List<String> alpha_type_list = new ArrayList<String>();
            int startIndex = (int) 'a';
            for (int i = 0; i < DATA_TYPE_NON_PRIMITIVE_NAMES.size(); i++)
                for (int j = 0; j < DATA_TYPE_PRIMITIVES.size(); j++) {
                    String typeString;
                    if(DATA_TYPE_NON_PRIMITIVE_NAMES.get(i) == DataType.Name.MAP) {
                        typeString = (String.format("%s_%s %s<%s, %s>", Character.toString((char) (startIndex + i)),
                                Character.toString((char) (startIndex + j)), DATA_TYPE_NON_PRIMITIVE_NAMES.get(i),
                                DATA_TYPE_PRIMITIVES.get(j).getName(), DATA_TYPE_PRIMITIVES.get(j).getName()));
                    }
                    else {
                        typeString = (String.format("%s_%s %s<%s>", Character.toString((char) (startIndex + i)),
                                Character.toString((char) (startIndex + j)), DATA_TYPE_NON_PRIMITIVE_NAMES.get(i),
                                DATA_TYPE_PRIMITIVES.get(j).getName()));
                    }
                    alpha_type_list.add(typeString);
                }

            session.execute(String.format("CREATE TYPE alldatatypes (%s)", Joiner.on(',').join(alpha_type_list)));
            session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b alldatatypes)");

            // insert UDT data
            UserType alldatatypesDef = cluster.getMetadata().getKeyspace("test_nonprimitive_datatypes").getUserType("alldatatypes");
            UDTValue alldatatypes = alldatatypesDef.newValue();

            for (int i = 0; i < DATA_TYPE_NON_PRIMITIVE_NAMES.size(); i++)
                for (int j = 0; j < DATA_TYPE_PRIMITIVES.size(); j++) {
                    DataType.Name name = DATA_TYPE_NON_PRIMITIVE_NAMES.get(i);
                    DataType dataType = DATA_TYPE_PRIMITIVES.get(j);

                    String index = Character.toString((char) (startIndex + i)) + "_" + Character.toString((char) (startIndex + j));
                    Object sample = DataTypeIntegrationTest.getCollectionSample(name, dataType);
                    switch(name) {
                        case LIST:
                            alldatatypes.setList(index, (ArrayList<DataType>) sample);
                            break;
                        case SET:
                            alldatatypes.setSet(index, (Set<DataType>) sample);
                            break;
                        case MAP:
                            alldatatypes.setMap(index, (HashMap<DataType, DataType>) sample);
                            break;
                        case TUPLE:
                            alldatatypes.setTupleValue(index, (TupleValue) sample);
                    }
                }

            PreparedStatement ins = session.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)");
            session.execute(ins.bind(0, alldatatypes));

            // retrieve and verify data
            ResultSet rs = session.execute("SELECT * FROM mytable");
            List<Row> rows = rs.all();
            assertEquals(1, rows.size());

            Row row = rows.get(0);

            assertEquals(row.getInt("a"), 0);
            assertEquals(row.getUDTValue("b"), alldatatypes);

        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }

    /**
     * Test for ensuring nested UDT's are handled correctly.
     * Original code found in python-driver:integration.standard.test_udts.py:test_nested_registered_udts
     * @throws Exception
     */
    @Test(groups = "short")
    public void udtNestedTest() throws Exception {
        final int MAX_NESTING_DEPTH = 4;

        try {
            // create keyspace
            session.execute("CREATE KEYSPACE udtNestedTest " +
                    "WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}");
            session.execute("USE udtNestedTest");

            // create UDT
            session.execute("CREATE TYPE depth_0 (age int, name text)");

            for (int i = 1; i <= MAX_NESTING_DEPTH; i++) {
                session.execute(String.format("CREATE TYPE depth_%s (value depth_%s)", String.valueOf(i), String.valueOf(i-1)));
            }

            session.execute(String.format("CREATE TABLE mytable (a int PRIMARY KEY, b depth_0, c depth_1, d depth_2, e depth_3," +
                    "f depth_%s)", MAX_NESTING_DEPTH));

            // insert UDT data
            UserType depthZeroDef = cluster.getMetadata().getKeyspace("udtNestedTest").getUserType("depth_0");
            UDTValue depthZero = depthZeroDef.newValue().setInt("age", 42).setString("name", "Bob");

            UserType depthOneDef = cluster.getMetadata().getKeyspace("udtNestedTest").getUserType("depth_1");
            UDTValue depthOne = depthOneDef.newValue().setUDTValue("value", depthZero);

            UserType depthTwoDef = cluster.getMetadata().getKeyspace("udtNestedTest").getUserType("depth_2");
            UDTValue depthTwo = depthTwoDef.newValue().setUDTValue("value", depthOne);

            UserType depthThreeDef = cluster.getMetadata().getKeyspace("udtNestedTest").getUserType("depth_3");
            UDTValue depthThree = depthThreeDef.newValue().setUDTValue("value", depthTwo);

            UserType depthFourDef = cluster.getMetadata().getKeyspace("udtNestedTest").getUserType("depth_4");
            UDTValue depthFour = depthFourDef.newValue().setUDTValue("value", depthThree);

            PreparedStatement ins = session.prepare("INSERT INTO mytable (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)");
            session.execute(ins.bind(0, depthZero, depthOne, depthTwo, depthThree, depthFour));

            // retrieve and verify data
            ResultSet rs = session.execute("SELECT * FROM mytable");
            List<Row> rows = rs.all();
            assertEquals(1, rows.size());

            Row row = rows.get(0);

            assertEquals(row.getInt("a"), 0);
            assertEquals(row.getUDTValue("b"), depthZero);
            assertEquals(row.getUDTValue("c"), depthOne);
            assertEquals(row.getUDTValue("d"), depthTwo);
            assertEquals(row.getUDTValue("e"), depthThree);
            assertEquals(row.getUDTValue("f"), depthFour);

        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }

    /**
     * Test for inserting null values into UDT's
     * Original code found in python-driver:integration.standard.test_udts.py:test_udts_with_nulls
     * @throws Exception
     */
    @Test(groups = "short")
    public void testUdtsWithNulls() throws Exception {
        try {
            // create keyspace
            session.execute("CREATE KEYSPACE testUdtsWithNulls " +
                    "WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}");
            session.execute("USE testUdtsWithNulls");

            // create UDT
            session.execute("CREATE TYPE user (a text, b int, c uuid, d blob)");
            session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b user)");

            // insert UDT data
            UserType userTypeDef = cluster.getMetadata().getKeyspace("testUdtsWithNulls").getUserType("user");
            UDTValue userType = userTypeDef.newValue().setString("a", null).setInt("b", 0).setUUID("c", null).setBytes("d", null);

            PreparedStatement ins = session.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)");
            session.execute(ins.bind(0, userType));

            // retrieve and verify data
            ResultSet rs = session.execute("SELECT * FROM mytable");
            List<Row> rows = rs.all();
            assertEquals(1, rows.size());

            Row row = rows.get(0);

            assertEquals(row.getInt("a"), 0);
            assertEquals(row.getUDTValue("b"), userType);

            // test empty strings
            userType = userTypeDef.newValue().setString("a", "").setInt("b", 0).setUUID("c", null).setBytes("d", ByteBuffer.allocate(0));
            session.execute(ins.bind(0, userType));

            // retrieve and verify data
            rs = session.execute("SELECT * FROM mytable");
            rows = rs.all();
            assertEquals(1, rows.size());

            row = rows.get(0);

            assertEquals(row.getInt("a"), 0);
            assertEquals(row.getUDTValue("b"), userType);

        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }
}
