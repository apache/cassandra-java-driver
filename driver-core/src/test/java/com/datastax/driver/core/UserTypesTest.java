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

import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.ConditionChecker.check;
import static com.datastax.driver.core.Metadata.quote;
import static java.util.concurrent.TimeUnit.MINUTES;

@CassandraVersion("2.1.0")
public class UserTypesTest extends CCMTestsSupport {

    private List<DataType> DATA_TYPE_PRIMITIVES;
    private Map<DataType, Object> samples;

    private final static List<DataType.Name> DATA_TYPE_NON_PRIMITIVE_NAMES =
            new ArrayList<DataType.Name>(EnumSet.of(DataType.Name.LIST, DataType.Name.SET, DataType.Name.MAP, DataType.Name.TUPLE));

    private final Callable<Boolean> userTableExists = new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
            return cluster().getMetadata().getKeyspace(keyspace).getTable("user") != null;
        }
    };

    @Override
    public void onTestContextInitialized() {
        ProtocolVersion protocolVersion = ccm().getProtocolVersion();
        DATA_TYPE_PRIMITIVES = new ArrayList<DataType>(TestUtils.allPrimitiveTypes(protocolVersion));
        DATA_TYPE_PRIMITIVES.remove(DataType.counter());
        samples = PrimitiveTypeSamples.samples(protocolVersion);
        String type1 = "CREATE TYPE phone (alias text, number text)";
        String type2 = "CREATE TYPE \"\"\"User Address\"\"\" (street text, \"ZIP\"\"\" int, phones set<frozen<phone>>)";
        String type3 = "CREATE TYPE type_for_frozen_test(i int)";
        String table = "CREATE TABLE user (id int PRIMARY KEY, addr frozen<\"\"\"User Address\"\"\">)";
        execute(type1, type2, type3, table);
        // Ci tests fail with "unconfigured columnfamily user"
        check().that(userTableExists).before(5, MINUTES).becomesTrue();
    }

    @Test(groups = "short")
    public void should_store_and_retrieve_with_prepared_statements() throws Exception {
        int userId = 0;
        PreparedStatement ins = session().prepare("INSERT INTO user(id, addr) VALUES (?, ?)");
        PreparedStatement sel = session().prepare("SELECT * FROM user WHERE id=?");

        UserType addrDef = cluster().getMetadata().getKeyspace(keyspace).getUserType(quote("\"User Address\""));
        UserType phoneDef = cluster().getMetadata().getKeyspace(keyspace).getUserType("phone");

        UDTValue phone1 = phoneDef.newValue().setString("alias", "home").setString("number", "0123548790");
        UDTValue phone2 = phoneDef.newValue().setString("alias", "work").setString("number", "0698265251");

        UDTValue addr = addrDef.newValue().setString("street", "1600 Pennsylvania Ave NW").setInt(quote("ZIP\""), 20500).setSet("phones", ImmutableSet.of(phone1, phone2));

        session().execute(ins.bind(userId, addr));

        Row r = session().execute(sel.bind(userId)).one();

        assertThat(r.getInt("id")).isEqualTo(0);
    }

    @Test(groups = "short")
    public void should_store_and_retrieve_with_simple_statements() throws Exception {
        int userId = 1;
        UserType addrDef = cluster().getMetadata().getKeyspace(keyspace).getUserType(quote("\"User Address\""));
        UserType phoneDef = cluster().getMetadata().getKeyspace(keyspace).getUserType("phone");

        UDTValue phone1 = phoneDef.newValue().setString("alias", "home").setString("number", "0123548790");
        UDTValue phone2 = phoneDef.newValue().setString("alias", "work").setString("number", "0698265251");

        UDTValue addr = addrDef.newValue().setString("street", "1600 Pennsylvania Ave NW").setInt(quote("ZIP\""), 20500).setSet("phones", ImmutableSet.of(phone1, phone2));

        session().execute("INSERT INTO user(id, addr) VALUES (?, ?)", userId, addr);

        Row r = session().execute("SELECT * FROM user WHERE id=?", userId).one();

        assertThat(r.getInt("id")).isEqualTo(userId);
        assertThat(r.getUDTValue("addr")).isEqualTo(addr);
    }

    @Test(groups = "short")
    public void should_store_type_definitions_in_their_keyspace() throws Exception {
        KeyspaceMetadata thisKeyspace = cluster().getMetadata().getKeyspace(this.keyspace);

        // Types that don't exist don't have definitions
        assertThat(thisKeyspace.getUserType("address1"))
                .isNull();
        assertThat(thisKeyspace.getUserType("phone1"))
                .isNull();

        // Types created by this test have definitions
        assertThat(thisKeyspace.getUserType(quote("\"User Address\"")))
                .isNotNull();
        assertThat(thisKeyspace.getUserType("phone"))
                .isNotNull();

        // If we create another keyspace, it doesn't have the definitions of this keyspace
        String otherKeyspaceName = this.keyspace + "_nonEx";
        session().execute("CREATE KEYSPACE " + otherKeyspaceName + " " +
                "WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}");

        KeyspaceMetadata otherKeyspace = cluster().getMetadata().getKeyspace(otherKeyspaceName);
        assertThat(otherKeyspace.getUserType(quote("\"User Address\"")))
                .isNull();
        assertThat(otherKeyspace.getUserType("phone"))
                .isNull();
    }

    @Test(groups = "short")
    public void should_handle_UDT_with_many_fields() throws Exception {
        int MAX_TEST_LENGTH = 1024;

        // create the seed udt
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < MAX_TEST_LENGTH; ++i) {
            sb.append(String.format("v_%s int", i));

            if (i + 1 < MAX_TEST_LENGTH)
                sb.append(",");
        }
        session().execute(String.format("CREATE TYPE lengthy_udt (%s)", sb.toString()));

        // create a table with multiple sizes of udts
        session().execute("CREATE TABLE lengthy_udt_table (k int PRIMARY KEY, v frozen<lengthy_udt>)");

        // hold onto the UserType for future use
        UserType udtDef = cluster().getMetadata().getKeyspace(keyspace).getUserType("lengthy_udt");

        // verify inserts and reads
        for (int i : Arrays.asList(0, 1, 2, 3, MAX_TEST_LENGTH)) {
            // create udt
            UDTValue createdUDT = udtDef.newValue();
            for (int j = 0; j < i; ++j) {
                createdUDT.setInt(j, j);
            }

            // write udt
            session().execute("INSERT INTO lengthy_udt_table (k, v) VALUES (0, ?)", createdUDT);

            // verify udt was written and read correctly
            UDTValue r = session().execute("SELECT v FROM lengthy_udt_table WHERE k=0")
                    .one().getUDTValue("v");
            assertThat(r.toString()).isEqualTo(createdUDT.toString());
        }
    }

    @Test(groups = "short")
    public void should_store_and_retrieve_UDT_containing_any_primitive_type() throws Exception {
        // create UDT
        List<String> alpha_type_list = new ArrayList<String>();
        int startIndex = (int) 'a';
        for (int i = 0; i < DATA_TYPE_PRIMITIVES.size(); i++) {
            alpha_type_list.add(String.format("%s %s", Character.toString((char) (startIndex + i)),
                    DATA_TYPE_PRIMITIVES.get(i).getName()));
        }

        session().execute(String.format("CREATE TYPE alldatatypes (%s)", Joiner.on(',').join(alpha_type_list)));
        session().execute("CREATE TABLE alldatatypes_table (a int PRIMARY KEY, b frozen<alldatatypes>)");

        // insert UDT data
        UserType alldatatypesDef = cluster().getMetadata().getKeyspace(keyspace).getUserType("alldatatypes");
        UDTValue alldatatypes = alldatatypesDef.newValue();


        for (int i = 0; i < DATA_TYPE_PRIMITIVES.size(); i++) {
            DataType dataType = DATA_TYPE_PRIMITIVES.get(i);
            String index = Character.toString((char) (startIndex + i));
            Object sampleData = samples.get(dataType);

            switch (dataType.getName()) {
                case ASCII:
                    alldatatypes.setString(index, (String) sampleData);
                    break;
                case BIGINT:
                    alldatatypes.setLong(index, (Long) sampleData);
                    break;
                case BLOB:
                    alldatatypes.setBytes(index, (ByteBuffer) sampleData);
                    break;
                case BOOLEAN:
                    alldatatypes.setBool(index, (Boolean) sampleData);
                    break;
                case DECIMAL:
                    alldatatypes.setDecimal(index, (BigDecimal) sampleData);
                    break;
                case DOUBLE:
                    alldatatypes.setDouble(index, (Double) sampleData);
                    break;
                case DURATION:
                    alldatatypes.set(index, Duration.from(sampleData.toString()), Duration.class);
                    break;
                case FLOAT:
                    alldatatypes.setFloat(index, (Float) sampleData);
                    break;
                case INET:
                    alldatatypes.setInet(index, (InetAddress) sampleData);
                    break;
                case TINYINT:
                    alldatatypes.setByte(index, (Byte) sampleData);
                    break;
                case SMALLINT:
                    alldatatypes.setShort(index, (Short) sampleData);
                    break;
                case INT:
                    alldatatypes.setInt(index, (Integer) sampleData);
                    break;
                case TEXT:
                    alldatatypes.setString(index, (String) sampleData);
                    break;
                case TIMESTAMP:
                    alldatatypes.setTimestamp(index, ((Date) sampleData));
                    break;
                case DATE:
                    alldatatypes.setDate(index, ((LocalDate) sampleData));
                    break;
                case TIME:
                    alldatatypes.setTime(index, ((Long) sampleData));
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

        PreparedStatement ins = session().prepare("INSERT INTO alldatatypes_table (a, b) VALUES (?, ?)");
        session().execute(ins.bind(0, alldatatypes));

        // retrieve and verify data
        ResultSet rs = session().execute("SELECT * FROM alldatatypes_table");
        List<Row> rows = rs.all();
        assertThat(rows.size()).isEqualTo(1);

        Row row = rows.get(0);

        assertThat(row.getInt("a")).isEqualTo(0);
        assertThat(row.getUDTValue("b")).isEqualTo(alldatatypes);
    }

    @Test(groups = "short")
    public void should_store_and_retrieve_UDT_containing_collections_and_tuples() throws Exception {
        // counters and durations are not allowed inside collections
        DATA_TYPE_PRIMITIVES.remove(DataType.counter());
        DATA_TYPE_PRIMITIVES.remove(DataType.duration());

        // create UDT
        List<String> alpha_type_list = new ArrayList<String>();
        int startIndex = (int) 'a';
        for (int i = 0; i < DATA_TYPE_NON_PRIMITIVE_NAMES.size(); i++)
            for (int j = 0; j < DATA_TYPE_PRIMITIVES.size(); j++) {
                String typeString;
                if (DATA_TYPE_NON_PRIMITIVE_NAMES.get(i) == DataType.Name.MAP) {
                    typeString = (String.format("%s_%s %s<%s, %s>", Character.toString((char) (startIndex + i)),
                            Character.toString((char) (startIndex + j)), DATA_TYPE_NON_PRIMITIVE_NAMES.get(i),
                            DATA_TYPE_PRIMITIVES.get(j).getName(), DATA_TYPE_PRIMITIVES.get(j).getName()));
                } else if (DATA_TYPE_NON_PRIMITIVE_NAMES.get(i) == DataType.Name.TUPLE) {
                    typeString = (String.format("%s_%s frozen<%s<%s>>", Character.toString((char) (startIndex + i)),
                            Character.toString((char) (startIndex + j)), DATA_TYPE_NON_PRIMITIVE_NAMES.get(i),
                            DATA_TYPE_PRIMITIVES.get(j).getName()));
                } else {
                    typeString = (String.format("%s_%s %s<%s>", Character.toString((char) (startIndex + i)),
                            Character.toString((char) (startIndex + j)), DATA_TYPE_NON_PRIMITIVE_NAMES.get(i),
                            DATA_TYPE_PRIMITIVES.get(j).getName()));
                }
                alpha_type_list.add(typeString);
            }

        session().execute(String.format("CREATE TYPE allcollectiontypes (%s)", Joiner.on(',').join(alpha_type_list)));
        session().execute("CREATE TABLE allcollectiontypes_table (a int PRIMARY KEY, b frozen<allcollectiontypes>)");

        // insert UDT data
        UserType allcollectiontypesDef = cluster().getMetadata().getKeyspace(keyspace).getUserType("allcollectiontypes");
        UDTValue allcollectiontypes = allcollectiontypesDef.newValue();

        for (int i = 0; i < DATA_TYPE_NON_PRIMITIVE_NAMES.size(); i++)
            for (int j = 0; j < DATA_TYPE_PRIMITIVES.size(); j++) {
                DataType.Name name = DATA_TYPE_NON_PRIMITIVE_NAMES.get(i);
                DataType dataType = DATA_TYPE_PRIMITIVES.get(j);

                String index = Character.toString((char) (startIndex + i)) + "_" + Character.toString((char) (startIndex + j));
                Object sampleElement = samples.get(dataType);
                switch (name) {
                    case LIST:
                        allcollectiontypes.setList(index, Lists.newArrayList(sampleElement));
                        break;
                    case SET:
                        allcollectiontypes.setSet(index, Sets.newHashSet(sampleElement));
                        break;
                    case MAP:
                        allcollectiontypes.setMap(index, ImmutableMap.of(sampleElement, sampleElement));
                        break;
                    case TUPLE:
                        allcollectiontypes.setTupleValue(index, cluster().getMetadata().newTupleType(dataType).newValue(sampleElement));
                }
            }

        PreparedStatement ins = session().prepare("INSERT INTO allcollectiontypes_table (a, b) VALUES (?, ?)");
        session().execute(ins.bind(0, allcollectiontypes));

        // retrieve and verify data
        ResultSet rs = session().execute("SELECT * FROM allcollectiontypes_table");
        List<Row> rows = rs.all();
        assertThat(rows.size()).isEqualTo(1);

        Row row = rows.get(0);

        assertThat(row.getInt("a")).isEqualTo(0);
        assertThat(row.getUDTValue("b")).isEqualTo(allcollectiontypes);
    }

    @Test(groups = "short")
    public void should_save_and_retrieve_nested_UDTs() throws Exception {
        final int MAX_NESTING_DEPTH = 4;

        // create UDT
        session().execute("CREATE TYPE depth_0 (age int, name text)");

        for (int i = 1; i <= MAX_NESTING_DEPTH; i++) {
            session().execute(String.format("CREATE TYPE depth_%s (value frozen<depth_%s>)", String.valueOf(i), String.valueOf(i - 1)));
        }

        session().execute(String.format("CREATE TABLE nested_udt_table (a int PRIMARY KEY, b frozen<depth_0>, c frozen<depth_1>, d frozen<depth_2>, e frozen<depth_3>," +
                "f frozen<depth_%s>)", MAX_NESTING_DEPTH));

        // insert UDT data
        KeyspaceMetadata keyspaceMetadata = cluster().getMetadata().getKeyspace(keyspace);
        UserType depthZeroDef = keyspaceMetadata.getUserType("depth_0");
        UDTValue depthZero = depthZeroDef.newValue().setInt("age", 42).setString("name", "Bob");

        UserType depthOneDef = keyspaceMetadata.getUserType("depth_1");
        UDTValue depthOne = depthOneDef.newValue().setUDTValue("value", depthZero);

        UserType depthTwoDef = keyspaceMetadata.getUserType("depth_2");
        UDTValue depthTwo = depthTwoDef.newValue().setUDTValue("value", depthOne);

        UserType depthThreeDef = keyspaceMetadata.getUserType("depth_3");
        UDTValue depthThree = depthThreeDef.newValue().setUDTValue("value", depthTwo);

        UserType depthFourDef = keyspaceMetadata.getUserType("depth_4");
        UDTValue depthFour = depthFourDef.newValue().setUDTValue("value", depthThree);

        PreparedStatement ins = session().prepare("INSERT INTO nested_udt_table (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)");
        session().execute(ins.bind(0, depthZero, depthOne, depthTwo, depthThree, depthFour));

        // retrieve and verify data
        ResultSet rs = session().execute("SELECT * FROM nested_udt_table");
        List<Row> rows = rs.all();
        assertThat(rows.size()).isEqualTo(1);

        Row row = rows.get(0);

        assertThat(row.getInt("a")).isEqualTo(0);
        assertThat(row.getUDTValue("b")).isEqualTo(depthZero);
        assertThat(row.getUDTValue("c")).isEqualTo(depthOne);
        assertThat(row.getUDTValue("d")).isEqualTo(depthTwo);
        assertThat(row.getUDTValue("e")).isEqualTo(depthThree);
        assertThat(row.getUDTValue("f")).isEqualTo(depthFour);
    }

    @Test(groups = "short")
    public void should_save_and_retrieve_UDTs_with_null_values() throws Exception {
        // create UDT
        session().execute("CREATE TYPE user_null_values (a text, b int, c uuid, d blob)");
        session().execute("CREATE TABLE null_values_table (a int PRIMARY KEY, b frozen<user_null_values>)");

        // insert UDT data
        UserType userTypeDef = cluster().getMetadata().getKeyspace(keyspace).getUserType("user_null_values");
        UDTValue userType = userTypeDef.newValue().setString("a", null).setInt("b", 0).setUUID("c", null).setBytes("d", null);

        PreparedStatement ins = session().prepare("INSERT INTO null_values_table (a, b) VALUES (?, ?)");
        session().execute(ins.bind(0, userType));

        // retrieve and verify data
        ResultSet rs = session().execute("SELECT * FROM null_values_table");
        List<Row> rows = rs.all();
        assertThat(rows.size()).isEqualTo(1);

        Row row = rows.get(0);

        assertThat(row.getInt("a")).isEqualTo(0);
        assertThat(row.getUDTValue("b")).isEqualTo(userType);

        // test empty strings
        userType = userTypeDef.newValue().setString("a", "").setInt("b", 0).setUUID("c", null).setBytes("d", ByteBuffer.allocate(0));
        session().execute(ins.bind(0, userType));

        // retrieve and verify data
        rs = session().execute("SELECT * FROM null_values_table");
        rows = rs.all();
        assertThat(rows.size()).isEqualTo(1);

        row = rows.get(0);

        assertThat(row.getInt("a")).isEqualTo(0);
        assertThat(row.getUDTValue("b")).isEqualTo(userType);
    }

    @Test(groups = "short")
    public void should_save_and_retrieve_UDTs_with_null_collections() throws Exception {
        // create UDT
        session().execute("CREATE TYPE user_null_collections (a List<text>, b Set<text>, c Map<text, text>, d frozen<Tuple<text>>)");
        session().execute("CREATE TABLE null_collections_table (a int PRIMARY KEY, b frozen<user_null_collections>)");

        // insert null UDT data
        PreparedStatement ins = session().prepare("INSERT INTO null_collections_table (a, b) " +
                "VALUES (0, { a: ?, b: ?, c: ?, d: ? })");
        session().execute(ins.bind().setList(0, null).setSet(1, null).setMap(2, null).setTupleValue(3, null));

        // retrieve and verify data
        ResultSet rs = session().execute("SELECT * FROM null_collections_table");
        List<Row> rows = rs.all();
        assertThat(rows.size()).isEqualTo(1);

        Row row = rows.get(0);
        assertThat(row.getInt("a")).isEqualTo(0);

        UserType userTypeDef = cluster().getMetadata().getKeyspace(keyspace).getUserType("user_null_collections");
        UDTValue userType = userTypeDef.newValue().setList("a", null).setSet("b", null).setMap("c", null).setTupleValue("d", null);
        assertThat(row.getUDTValue("b")).isEqualTo(userType);

        // test missing UDT args
        ins = session().prepare("INSERT INTO null_collections_table (a, b) " +
                "VALUES (1, { a: ? })");
        session().execute(ins.bind().setList(0, new ArrayList<Object>()));

        // retrieve and verify data
        rs = session().execute("SELECT * FROM null_collections_table");
        rows = rs.all();
        assertThat(rows.size()).isEqualTo(2);

        row = rows.get(0);
        assertThat(row.getInt("a")).isEqualTo(1);

        userType = userTypeDef.newValue().setList(0, new ArrayList<Object>());
        assertThat(row.getUDTValue("b")).isEqualTo(userType);
    }

    @Test(groups = "short")
    public void should_indicate_user_type_is_frozen() {
        session().execute("CREATE TABLE frozen_table(k int primary key, v frozen<type_for_frozen_test>)");

        KeyspaceMetadata keyspaceMetadata = cluster().getMetadata().getKeyspace(this.keyspace);

        assertThat(keyspaceMetadata.getUserType("type_for_frozen_test"))
                .isNotFrozen();

        DataType userType = keyspaceMetadata.getTable("frozen_table").getColumn("v").getType();
        assertThat(userType).isFrozen();
        assertThat(userType.toString()).isEqualTo("frozen<" + keyspace + ".type_for_frozen_test>");

        // The frozen flag is not set for result set definitions (the protocol does not provide
        // that information and it's not really useful in that situation). We always return false.
        ResultSet rs = session().execute("SELECT v FROM frozen_table WHERE k = 1");
        assertThat(rs.getColumnDefinitions().getType(0))
                .isNotFrozen();

        // Same thing for prepared statements
        PreparedStatement pst = session().prepare("SELECT v FROM frozen_table WHERE k = ?");
        assertThat(pst.getVariables().getType(0))
                .isNotFrozen();
    }

    @Test(groups = "short")
    @CassandraVersion(value = "3.6", description = "Non-frozen UDTs were introduced in C* 3.6")
    public void should_indicate_user_type_is_not_frozen() {
        session().execute("CREATE TABLE not_frozen_table(k int primary key, v type_for_frozen_test)");

        KeyspaceMetadata keyspaceMetadata = cluster().getMetadata().getKeyspace(this.keyspace);

        assertThat(keyspaceMetadata.getUserType("type_for_frozen_test"))
                .isNotFrozen();

        DataType userType = keyspaceMetadata.getTable("not_frozen_table").getColumn("v").getType();
        assertThat(userType).isNotFrozen();
        assertThat(userType.toString()).isEqualTo(keyspace + ".type_for_frozen_test");

        ResultSet rs = session().execute("SELECT v FROM not_frozen_table WHERE k = 1");
        assertThat(rs.getColumnDefinitions().getType(0))
                .isNotFrozen();

        PreparedStatement pst = session().prepare("SELECT v FROM not_frozen_table WHERE k = ?");
        assertThat(pst.getVariables().getType(0))
                .isNotFrozen();
    }
}
