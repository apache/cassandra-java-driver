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

import java.lang.Exception;
import java.util.*;

import com.google.common.collect.ImmutableSet;
import com.google.common.base.Joiner;

import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

import static com.datastax.driver.core.Metadata.quote;
import static com.datastax.driver.core.TestUtils.versionCheck;

public class UserTypesTest extends CCMBridge.PerClassSingleNodeCluster {

    private final static Set<DataType> DATA_TYPE_PRIMITIVES = DataType.allPrimitiveTypes();
    private final static Set<DataType.Name> DATA_TYPE_NON_PRIMITIVE_NAMES = EnumSet.of(DataType.Name.MAP, DataType.Name.SET, DataType.Name.LIST);

    private final static HashMap<DataType, Object> SAMPLE_DATA = DataTypeIntegrationTest.getSampleData();
    private final static HashMap<DataType, Object> SAMPLE_COLLECTIONS = DataTypeIntegrationTest.getSampleCollections();

    @Override
    protected Collection<String> getTableDefinitions() {
        versionCheck(2.1, 0, "This will only work with Cassandra 2.1.0");

        String type1 = "CREATE TYPE phone (alias text, number text)";
        String type2 = "CREATE TYPE address (street text, \"ZIP\" int, phones set<phone>)";

        String table = "CREATE TABLE user (id int PRIMARY KEY, addr address)";

        return Arrays.asList(type1, type2, table);
    }
//
//    @Test(groups = "short")
//    public void simpleWriteReadTest() throws Exception {
//        int userId = 0;
//
//        try {
//            PreparedStatement ins = session.prepare("INSERT INTO user(id, addr) VALUES (?, ?)");
//            PreparedStatement sel = session.prepare("SELECT * FROM user WHERE id=?");
//
//            UserType addrDef = cluster.getMetadata().getKeyspace("ks").getUserType("address");
//            UserType phoneDef = cluster.getMetadata().getKeyspace("ks").getUserType("phone");
//
//            UDTValue phone1 = phoneDef.newValue().setString("alias", "home").setString("number", "0123548790");
//            UDTValue phone2 = phoneDef.newValue().setString("alias", "work").setString("number", "0698265251");
//
//            UDTValue addr = addrDef.newValue().setString("street", "1600 Pennsylvania Ave NW").setInt(quote("ZIP"), 20500).setSet("phones", ImmutableSet.of(phone1, phone2));
//
//            session.execute(ins.bind(userId, addr));
//
//            Row r = session.execute(sel.bind(userId)).one();
//
//            assertEquals(r.getInt("id"), 0);
//            assertEquals(r.getUDTValue("addr"), addr);
//        } catch (Exception e) {
//            errorOut();
//            throw e;
//        }
//    }

    /**
     * Test for inserting various types of DATA_TYPE_PRIMITIVES into UDT's.
     * Original code found in python-driver:integration.standard.test_types.py:test_udts
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
            for (int i = 0; i < DATA_TYPE_PRIMITIVES.length(); i++) {
                alpha_type_list.add(String.format("%s %s", Character.toString((char) startIndex + i),
                        DATA_TYPE_PRIMITIVES.get(i).getName()));
            }

            session.execute(String.format("CREATE TYPE alldatatypes (%s)", Joiner.on(',').join(alpha_type_list)));
            session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b alldatatypes)");

            // insert UDT data
            UserType alldatatypesDef = cluster.getMetadata().getKeyspace("testPrimitiveDatatypes").getUserType("alldatatypes");
            UDTValue alldatatypes = alldatatypesDef.newValue();

            int startIndex = (int) 'a';
            for (int i = 0; i < DATA_TYPE_PRIMITIVES.length(); i++) {
                Datatype datatype = DATA_TYPE_PRIMITIVES.get(i);
                switch (dataType.getName()) {
                    case ASCII:
                        alldatatypes.setString(Character.toString((char) startIndex + i), SAMPLE_DATA[datatype]);
                        break;
                    case BIGINT:
                        alldatatypes.setLong(Character.toString((char) startIndex + i), SAMPLE_DATA[datatype]);
                        break;
                    case BLOB:
                        alldatatypes.setBytes(Character.toString((char) startIndex + i), SAMPLE_DATA[datatype]);
                        break;
                    case BOOLEAN:
                        alldatatypes.setBool(Character.toString((char) startIndex + i), SAMPLE_DATA[datatype]);
                        break;
                    case DECIMAL:
                        alldatatypes.setDecimal(Character.toString((char) startIndex + i), SAMPLE_DATA[datatype]);
                        break;
                    case DOUBLE:
                        alldatatypes.setDouble(Character.toString((char) startIndex + i), SAMPLE_DATA[datatype]);
                        break;
                    case FLOAT:
                        alldatatypes.setFloat(Character.toString((char) startIndex + i), SAMPLE_DATA[datatype]);
                        break;
                    case INET:
                        alldatatypes.setInet(Character.toString((char) startIndex + i), SAMPLE_DATA[datatype]);
                        break;
                    case INT:
                        alldatatypes.setInt(Character.toString((char) startIndex + i), SAMPLE_DATA[datatype]);
                        break;
                    case TEXT:
                        alldatatypes.setString(Character.toString((char) startIndex + i), SAMPLE_DATA[datatype]);
                        break;
                    case TIMESTAMP:
                        alldatatypes.setDate(Character.toString((char) startIndex + i), SAMPLE_DATA[datatype]);
                        break;
                    case TIMEUUID:
                        alldatatypes.setUUID(Character.toString((char) startIndex + i), SAMPLE_DATA[datatype]);
                        break;
                    case UUID:
                        alldatatypes.setUUID(Character.toString((char) startIndex + i), SAMPLE_DATA[datatype]);
                        break;
                    case VARCHAR:
                        alldatatypes.setString(Character.toString((char) startIndex + i), SAMPLE_DATA[datatype]);
                        break;
                    case VARINT:
                        alldatatypes.setVarint(Character.toString((char) startIndex + i), SAMPLE_DATA[datatype]);
                        break;
                }
            }

            PreparedStatement ins = session.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)");
            session.execute(ins.bind(0, alldatatypes));

            // retrieve and verify data
            Rows rows = session.execute("SELECT * FROM mytable")
            assertEquals(1, rows.length);

            Row row = rows.one();

            assertEquals(row.getInt("a"), 0);
            assertEquals(row.getUDTValue("alldatatypes"), alldatatypes);

        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }
}
