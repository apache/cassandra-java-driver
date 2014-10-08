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

import java.util.*;
import java.nio.ByteBuffer;

import com.google.common.base.Joiner;
import org.testng.annotations.Test;

import static com.datastax.driver.core.DataTypeIntegrationTest.getSampleData;
import static com.datastax.driver.core.TestUtils.SIMPLE_KEYSPACE;
import static com.datastax.driver.core.TestUtils.versionCheck;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TupleTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        versionCheck(2.1, 0, "This will only work with Cassandra 2.1.0");

        return Arrays.asList("CREATE TABLE t (k int PRIMARY KEY, v frozen<tuple<int, text, float>>)");
    }

    @Test(groups = "short")
    public void simpleValueTest() throws Exception {
        TupleType t = TupleType.of(DataType.cint(), DataType.text(), DataType.cfloat());
        TupleValue v = t.newValue();
        v.setInt(0, 1);
        v.setString(1, "a");
        v.setFloat(2, 1.0f);

        assertEquals(v.getType().getComponentTypes().size(), 3);
        assertEquals(v.getType().getComponentTypes().get(0), DataType.cint());
        assertEquals(v.getType().getComponentTypes().get(1), DataType.text());
        assertEquals(v.getType().getComponentTypes().get(2), DataType.cfloat());

        assertEquals(v.getInt(0), 1);
        assertEquals(v.getString(1), "a");
        assertEquals(v.getFloat(2), 1.0f);

        assertEquals(t.format(v), "(1, 'a', 1.0)");
    }

    @Test(groups = "short")
    public void simpleWriteReadTest() throws Exception {
        try {
            session.execute("USE " + SIMPLE_KEYSPACE);
            PreparedStatement ins = session.prepare("INSERT INTO t(k, v) VALUES (?, ?)");
            PreparedStatement sel = session.prepare("SELECT * FROM t WHERE k=?");

            TupleType t = TupleType.of(DataType.cint(), DataType.text(), DataType.cfloat());

            int k = 1;
            TupleValue v = t.newValue(1, "a", 1.0f);

            session.execute(ins.bind(k, v));
            TupleValue v2 = session.execute(sel.bind(k)).one().getTupleValue("v");

            assertEquals(v2, v);

            // Test simple statement interpolation
            k = 2;
            v = t.newValue(2, "b", 2.0f);

            session.execute("INSERT INTO t(k, v) VALUES (?, ?)", k, v);
            v2 = session.execute(sel.bind(k)).one().getTupleValue("v");

            assertEquals(v2, v);
        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }

    /**
     * Basic test of tuple functionality.
     * Original code found in python-driver:integration.standard.test_types.py:test_tuple_type
     * @throws Exception
     */
    @Test(groups = "short")
    public void tupleTypeTest() throws Exception {
        try {
            session.execute("CREATE KEYSPACE test_tuple_type " +
                            "WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}");
            session.execute("USE test_tuple_type");
            session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<tuple<ascii, int, boolean>>)");

            TupleType t = TupleType.of(DataType.ascii(), DataType.cint(), DataType.cboolean());

            // test non-prepared statement
            TupleValue complete = t.newValue("foo", 123, true);
            session.execute("INSERT INTO mytable (a, b) VALUES (0, ?)", complete);
            TupleValue r = session.execute("SELECT b FROM mytable WHERE a=0").one().getTupleValue("b");
            assertEquals(r, complete);

            // test incomplete tuples
            try {
                TupleValue partial = t.newValue("bar", 456);
                fail();
            } catch (IllegalArgumentException e) {}

            // test incomplete tuples with new TupleType
            TupleType t1 = TupleType.of(DataType.ascii(), DataType.cint());
            TupleValue partial = t1.newValue("bar", 456);
            TupleValue partionResult = t.newValue("bar", 456, null);
            session.execute("INSERT INTO mytable (a, b) VALUES (0, ?)", partial);
            r = session.execute("SELECT b FROM mytable WHERE a=0").one().getTupleValue("b");
            assertEquals(r, partionResult);

            // test single value tuples
            try {
                TupleValue subpartial = t.newValue("zoo");
                fail();
            } catch (IllegalArgumentException e) {}

            // test single value tuples with new TupleType
            TupleType t2 = TupleType.of(DataType.ascii());
            TupleValue subpartial = t2.newValue("zoo");
            TupleValue subpartialResult = t.newValue("zoo", null, null);
            session.execute("INSERT INTO mytable (a, b) VALUES (0, ?)", subpartial);
            r = session.execute("SELECT b FROM mytable WHERE a=0").one().getTupleValue("b");
            assertEquals(r, subpartialResult);

            // test prepared statements
            PreparedStatement prepared = session.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)");
            session.execute(prepared.bind(3, complete));
            session.execute(prepared.bind(4, partial));
            session.execute(prepared.bind(5, subpartial));

            prepared = session.prepare("SELECT b FROM mytable WHERE a=?");
            assertEquals(session.execute(prepared.bind(3)).one().getTupleValue("b"), complete);
            assertEquals(session.execute(prepared.bind(4)).one().getTupleValue("b"), partionResult);
            assertEquals(session.execute(prepared.bind(5)).one().getTupleValue("b"), subpartialResult);

        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }

    /**
     * Test tuple types of lengths of 1, 2, 3, and 384 to ensure edge cases work
     * as expected.
     * Original code found in python-driver:integration.standard.test_types.py:test_tuple_type_varying_lengths
     *
     * @throws Exception
     */
    @Test(groups = "short")
    public void tupleTestTypeVaryingLengths() throws Exception {
        try {
            session.execute("CREATE KEYSPACE test_tuple_type_varying_lengths " +
                            "WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}");
            session.execute("USE test_tuple_type_varying_lengths");

            // programmatically create the table with tuples of said sizes
            int[] lengths = {1, 2, 3, 384};
            ArrayList<String> valueSchema = new ArrayList<String>();
            for (int i : lengths) {
                ArrayList<String> ints = new ArrayList<String>();
                for (int j = 0; j < i; ++j) {
                    ints.add("int");
                }
                valueSchema.add(String.format(" v_%d frozen<tuple<%s>>", i, Joiner.on(',').join(ints)));
            }
            session.execute(String.format("CREATE TABLE mytable (k int PRIMARY KEY, %s)", Joiner.on(',').join(valueSchema)));

            // insert tuples into same key using different columns
            // and verify the results
            for (int i : lengths) {
                // create tuple
                ArrayList<DataType> dataTypes = new ArrayList<DataType>();
                ArrayList<Integer> values = new ArrayList<Integer>();
                for (int j = 0; j < i; ++j) {
                    dataTypes.add(DataType.cint());
                    values.add(j);
                }
                TupleType t = new TupleType(dataTypes);
                TupleValue createdTuple = t.newValue(values.toArray());

                // write tuple
                session.execute(String.format("INSERT INTO mytable (k, v_%s) VALUES (0, ?)", i), createdTuple);

                // read tuple
                TupleValue r = session.execute(String.format("SELECT v_%s FROM mytable WHERE k=0", i)).one().getTupleValue(String.format("v_%s", i));
                assertEquals(r, createdTuple);
            }

        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }

    /**
     * Ensure tuple subtypes are appropriately handled.
     * Original code found in python-driver:integration.standard.test_types.py:test_tuple_subtypes
     *
     * @throws Exception
     */
    @Test(groups = "short")
    public void tupleSubtypesTest() throws Exception {

        // hold onto constants
        ArrayList<DataType> DATA_TYPE_PRIMITIVES = new ArrayList<DataType>();
        for (DataType dt : DataType.allPrimitiveTypes()) {
            // skip counter types since counters are not allowed inside tuples
            if (dt == DataType.counter())
                continue;

            DATA_TYPE_PRIMITIVES.add(dt);
        }
        HashMap<DataType, Object> SAMPLE_DATA = getSampleData();

        try {
            session.execute("CREATE KEYSPACE test_tuple_subtypes " +
                            "WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}");
            session.execute("USE test_tuple_subtypes");

            // programmatically create the table with a tuple of all datatypes
            session.execute(String.format("CREATE TABLE mytable (k int PRIMARY KEY, v frozen<tuple<%s>>)", Joiner.on(',').join(DATA_TYPE_PRIMITIVES)));

            // insert tuples into same key using different columns
            // and verify the results
            int i = 1;
            for (DataType datatype : DATA_TYPE_PRIMITIVES) {
                // create tuples to be written and ensure they match with the expected response
                // responses have trailing None values for every element that has not been written
                ArrayList<DataType> dataTypes = new ArrayList<DataType>();
                ArrayList<DataType> completeDataTypes = new ArrayList<DataType>();
                ArrayList<Object> createdValues = new ArrayList<Object>();
                ArrayList<Object> completeValues = new ArrayList<Object>();

                // create written portion of the arrays
                for (int j = 0; j < i; ++j) {
                    dataTypes.add(DATA_TYPE_PRIMITIVES.get(j));
                    completeDataTypes.add(DATA_TYPE_PRIMITIVES.get(j));
                    createdValues.add(SAMPLE_DATA.get(DATA_TYPE_PRIMITIVES.get(j)));
                    completeValues.add(SAMPLE_DATA.get(DATA_TYPE_PRIMITIVES.get(j)));
                }

                // complete portion of the arrays needed for trailing nulls
                for (int j = 0; j < DATA_TYPE_PRIMITIVES.size() - i; ++j) {
                    completeDataTypes.add(DATA_TYPE_PRIMITIVES.get(i + j));
                    completeValues.add(null);
                }

                // actually create the tuples
                TupleType t = new TupleType(dataTypes);
                TupleType t2 = new TupleType(completeDataTypes);
                TupleValue createdTuple = t.newValue(createdValues.toArray());
                TupleValue completeTuple = t2.newValue(completeValues.toArray());

                // write tuple
                session.execute(String.format("INSERT INTO mytable (k, v) VALUES (%s, ?)", i), createdTuple);

                // read tuple
                TupleValue r = session.execute("SELECT v FROM mytable WHERE k=?", i).one().getTupleValue("v");

                assertEquals(r, completeTuple);
                ++i;
            }

        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }

    /**
     * Ensure tuple subtypes are appropriately handled for maps, sets, and lists.
     * Original code found in python-driver:integration.standard.test_types.py:test_tuple_non_primitive_subtypes
     *
     * @throws Exception
     */
    @Test(groups = "short")
    public void tupleNonPrimitiveSubTypesTest() throws Exception {

        // hold onto constants
        ArrayList<DataType> DATA_TYPE_PRIMITIVES = new ArrayList<DataType>();
        for (DataType dt : DataType.allPrimitiveTypes()) {
            // skip counter types since counters are not allowed inside tuples
            if (dt == DataType.counter())
                continue;

            DATA_TYPE_PRIMITIVES.add(dt);
        }
        ArrayList<DataType.Name> DATA_TYPE_NON_PRIMITIVE_NAMES = new ArrayList<DataType.Name>(Arrays.asList(DataType.Name.MAP,
                                                                                                            DataType.Name.SET,
                                                                                                            DataType.Name.LIST));
        HashMap<DataType, Object> SAMPLE_DATA = getSampleData();

        try {
            session.execute("CREATE KEYSPACE test_tuple_non_primitive_subtypes " +
                            "WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}");
            session.execute("USE test_tuple_non_primitive_subtypes");

            ArrayList<String> values = new ArrayList<String>();

            //create list values
            for (DataType datatype : DATA_TYPE_PRIMITIVES) {
                values.add(String.format("v_%s frozen<tuple<list<%s>>>", values.size(), datatype));
            }

            // create set values
            for (DataType datatype : DATA_TYPE_PRIMITIVES) {
                values.add(String.format("v_%s frozen<tuple<set<%s>>>", values.size(), datatype));
            }

            // create map values
            for (DataType datatype : DATA_TYPE_PRIMITIVES) {
                DataType dataType1 = datatype;
                DataType dataType2 = datatype;
                values.add(String.format("v_%s frozen<tuple<map<%s, %s>>>", values.size(), dataType1, dataType2));
            }

            // create table
            session.execute(String.format("CREATE TABLE mytable (k int PRIMARY KEY, %s)", Joiner.on(',').join(values)));


            int i = 0;
            // test tuple<list<datatype>>
            for (DataType datatype : DATA_TYPE_PRIMITIVES) {
                // create tuple
                ArrayList<DataType> dataTypes = new ArrayList<DataType>();
                ArrayList<Object> createdValues = new ArrayList<Object>();

                dataTypes.add(DataType.list(datatype));
                createdValues.add(Arrays.asList(SAMPLE_DATA.get(datatype)));

                TupleType t = new TupleType(dataTypes);
                TupleValue createdTuple = t.newValue(createdValues.toArray());

                // write tuple
                session.execute(String.format("INSERT INTO mytable (k, v_%s) VALUES (0, ?)", i), createdTuple);

                // read tuple
                TupleValue r = session.execute(String.format("SELECT v_%s FROM mytable WHERE k=0", i))
                        .one().getTupleValue(String.format("v_%s", i));

                assertEquals(r, createdTuple);
                ++i;
            }

            // test tuple<set<datatype>>
            for (DataType datatype : DATA_TYPE_PRIMITIVES) {
                // create tuple
                ArrayList<DataType> dataTypes = new ArrayList<DataType>();
                ArrayList<Object> createdValues = new ArrayList<Object>();

                dataTypes.add(DataType.set(datatype));
                createdValues.add(new HashSet<Object>(Arrays.asList(SAMPLE_DATA.get(datatype))));

                TupleType t = new TupleType(dataTypes);
                TupleValue createdTuple = t.newValue(createdValues.toArray());

                // write tuple
                session.execute(String.format("INSERT INTO mytable (k, v_%s) VALUES (0, ?)", i), createdTuple);

                // read tuple
                TupleValue r = session.execute(String.format("SELECT v_%s FROM mytable WHERE k=0", i))
                        .one().getTupleValue(String.format("v_%s", i));

                assertEquals(r, createdTuple);
                ++i;
            }

            // test tuple<map<datatype, datatype>>
            for (DataType datatype : DATA_TYPE_PRIMITIVES) {
                // create tuple
                ArrayList<DataType> dataTypes = new ArrayList<DataType>();
                ArrayList<Object> createdValues = new ArrayList<Object>();

                HashMap<Object, Object> hm = new HashMap<Object, Object>();
                hm.put(SAMPLE_DATA.get(datatype), SAMPLE_DATA.get(datatype));

                dataTypes.add(DataType.map(datatype, datatype));
                createdValues.add(hm);

                TupleType t = new TupleType(dataTypes);
                TupleValue createdTuple = t.newValue(createdValues.toArray());

                // write tuple
                session.execute(String.format("INSERT INTO mytable (k, v_%s) VALUES (0, ?)", i), createdTuple);

                // read tuple
                TupleValue r = session.execute(String.format("SELECT v_%s FROM mytable WHERE k=0", i))
                        .one().getTupleValue(String.format("v_%s", i));

                assertEquals(r, createdTuple);
                ++i;
            }
        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }

    /**
     * Helper method for creating nested tuple schema
     * @param depth
     * @return
     */
    private String nestedTuplesSchemaHelper(int depth) {
        if (depth == 0)
            return "int";
        else
            return String.format("frozen<tuple<%s>>", nestedTuplesSchemaHelper(depth - 1));
    }

    /**
     * Helper method for creating nested tuples
     * @param depth
     * @return
     */
    private TupleValue nestedTuplesCreatorHelper(int depth) {
        if (depth == 1) {
            TupleType baseTuple = TupleType.of(DataType.cint());
            return baseTuple.newValue(303);
        } else {
            TupleValue innerTuple = nestedTuplesCreatorHelper(depth - 1);
            TupleType t = TupleType.of(innerTuple.getType());
            return t.newValue(innerTuple);
        }
    }

    /**
     * Ensure nested are appropriately handled.
     * Original code found in python-driver:integration.standard.test_types.py:test_nested_tuples
     *
     * @throws Exception
     */
    @Test(groups = "short")
    public void nestedTuplesTest() throws Exception {

        // hold onto constants
        ArrayList<DataType> DATA_TYPE_PRIMITIVES = new ArrayList<DataType>();
        for (DataType dt : DataType.allPrimitiveTypes()) {
            // skip counter types since counters are not allowed inside tuples
            if (dt == DataType.counter())
                continue;

            DATA_TYPE_PRIMITIVES.add(dt);
        }
        HashMap<DataType, Object> SAMPLE_DATA = getSampleData();

        try {
            session.execute("CREATE KEYSPACE test_nested_tuples " +
                    "WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}");
            session.execute("USE test_nested_tuples");

            // create a table with multiple sizes of nested tuples
            session.execute(String.format("CREATE TABLE mytable (" +
                            "k int PRIMARY KEY, " +
                            "v_1 %s, " +
                            "v_2 %s, " +
                            "v_3 %s, " +
                            "v_128 %s)", nestedTuplesSchemaHelper(1),
                                         nestedTuplesSchemaHelper(2),
                                         nestedTuplesSchemaHelper(3),
                                         nestedTuplesSchemaHelper(128)));

            for (int i : Arrays.asList(1, 2, 3, 128)) {
                // create tuple
                TupleValue createdTuple = nestedTuplesCreatorHelper(i);

                // write tuple
                session.execute(String.format("INSERT INTO mytable (k, v_%s) VALUES (?, ?)", i), i, createdTuple);

                // verify tuple was written and read correctly
                TupleValue r = session.execute(String.format("SELECT v_%s FROM mytable WHERE k=?", i), i)
                        .one().getTupleValue(String.format("v_%s", i));

                assertEquals(r, createdTuple);
            }
        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }

    /**
     * Test for inserting null Tuple values into UDT's
     * Original code found in python-driver:integration.standard.test_types.py:test_tuples_with_nulls
     * @throws Exception
     */
    @Test(groups = "short")
    public void testTuplesWithNulls() throws Exception {
        try {
            // create keyspace
            session.execute("CREATE KEYSPACE testTuplesWithNulls " +
                    "WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}");
            session.execute("USE testTuplesWithNulls");

            // create UDT
            session.execute("CREATE TYPE user (a text, b frozen<tuple<text, int, uuid, blob>>)");
            session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)");

            // insert UDT data
            UserType userTypeDef = cluster.getMetadata().getKeyspace("testTuplesWithNulls").getUserType("user");
            UDTValue userType = userTypeDef.newValue();

            TupleType t = TupleType.of(DataType.text(), DataType.cint(), DataType.uuid(), DataType.blob());
            TupleValue v = t.newValue(null, null, null, null);
            userType.setTupleValue("b", v);

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
            v = t.newValue("", null, null, ByteBuffer.allocate(0));
            userType.setTupleValue("b", v);
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
