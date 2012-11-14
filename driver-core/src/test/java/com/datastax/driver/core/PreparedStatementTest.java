package com.datastax.driver.core;

import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;

import com.datastax.driver.core.exceptions.*;
import static com.datastax.driver.core.TestUtils.*;

/**
 * Prepared statement tests.
 *
 * Note: this class also happens to test all the get methods from CQLRow.
 */
public class PreparedStatementTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final String ALL_NATIVE_TABLE = "all_native";
    private static final String ALL_LIST_TABLE = "all_list";
    private static final String ALL_SET_TABLE = "all_set";
    private static final String ALL_MAP_TABLE = "all_map";

    protected Collection<String> getTableDefinitions() {

        List<String> defs = new ArrayList<String>(4);

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(ALL_NATIVE_TABLE).append(" (k text PRIMARY KEY");
        for (DataType.Native type : DataType.Native.values()) {
            // This must be handled separatly
            if (type == DataType.Native.COUNTER)
                continue;
            sb.append(", c_").append(type).append(" ").append(type);
        }
        sb.append(")");
        defs.add(sb.toString());

        sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(ALL_LIST_TABLE).append(" (k text PRIMARY KEY");
        for (DataType.Native type : DataType.Native.values()) {
            // This must be handled separatly
            if (type == DataType.Native.COUNTER)
                continue;
            sb.append(", c_list_").append(type).append(" list<").append(type).append(">");
        }
        sb.append(")");
        defs.add(sb.toString());

        sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(ALL_SET_TABLE).append(" (k text PRIMARY KEY");
        for (DataType.Native type : DataType.Native.values()) {
            // This must be handled separatly
            if (type == DataType.Native.COUNTER)
                continue;
            sb.append(", c_set_").append(type).append(" set<").append(type).append(">");
        }
        sb.append(")");
        defs.add(sb.toString());

        sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(ALL_MAP_TABLE).append(" (k text PRIMARY KEY");
        for (DataType.Native keyType : DataType.Native.values()) {
            // This must be handled separatly
            if (keyType == DataType.Native.COUNTER)
                continue;

            for (DataType.Native valueType : DataType.Native.values()) {
                // This must be handled separatly
                if (valueType == DataType.Native.COUNTER)
                    continue;
                sb.append(", c_map_").append(keyType).append("_").append(valueType).append(" map<").append(keyType).append(",").append(valueType).append(">");
            }
        }
        sb.append(")");
        defs.add(sb.toString());

        return defs;
    }

    @Test
    public void preparedNativeTest() throws NoHostAvailableException {
        // Test preparing/bounding for all native types
        for (DataType.Native type : DataType.Native.values()) {
            // This must be handled separatly
            if (type == DataType.Native.COUNTER)
                continue;

            String name = "c_" + type;
            PreparedStatement ps = session.prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_native', ?)", ALL_NATIVE_TABLE, name));
            BoundStatement bs = ps.newBoundStatement();
            session.execute(setBoundValue(bs, name, type, getFixedValue(type)));

            CQLRow row = session.execute(String.format("SELECT %s FROM %s WHERE k='prepared_native'", name, ALL_NATIVE_TABLE)).fetchOne();
            assertEquals("For type " + type, getFixedValue(type), getValue(row, name, type));
        }
    }

    @Test
    public void prepareListTest() throws NoHostAvailableException {
        // Test preparing/bounding for all possible list types
        for (DataType.Native rawType : DataType.Native.values()) {
            // This must be handled separatly
            if (rawType == DataType.Native.COUNTER)
                continue;

            String name = "c_list_" + rawType;
            DataType type = new DataType.Collection.List(rawType);
            List value = (List)getFixedValue(type);;
            PreparedStatement ps = session.prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_list', ?)", ALL_LIST_TABLE, name));
            BoundStatement bs = ps.newBoundStatement();
            session.execute(setBoundValue(bs, name, type, value));

            CQLRow row = session.execute(String.format("SELECT %s FROM %s WHERE k='prepared_list'", name, ALL_LIST_TABLE)).fetchOne();
            assertEquals("For type " + type, value, getValue(row, name, type));
        }
    }

    @Test
    public void prepareSetTest() throws NoHostAvailableException {
        // Test preparing/bounding for all possible set types
        for (DataType.Native rawType : DataType.Native.values()) {
            // This must be handled separatly
            if (rawType == DataType.Native.COUNTER)
                continue;

            String name = "c_set_" + rawType;
            DataType type = new DataType.Collection.Set(rawType);
            Set value = (Set)getFixedValue(type);;
            PreparedStatement ps = session.prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_set', ?)", ALL_SET_TABLE, name));
            BoundStatement bs = ps.newBoundStatement();
            session.execute(setBoundValue(bs, name, type, value));

            CQLRow row = session.execute(String.format("SELECT %s FROM %s WHERE k='prepared_set'", name, ALL_SET_TABLE)).fetchOne();
            assertEquals("For type " + type, value, getValue(row, name, type));
        }
    }

    @Test
    public void prepareMapTest() throws NoHostAvailableException {
        // Test preparing/bounding for all possible map types
        for (DataType.Native rawKeyType : DataType.Native.values()) {
            // This must be handled separatly
            if (rawKeyType == DataType.Native.COUNTER)
                continue;

            for (DataType.Native rawValueType : DataType.Native.values()) {
                // This must be handled separatly
                if (rawValueType == DataType.Native.COUNTER)
                    continue;

                String name = "c_map_" + rawKeyType + "_" + rawValueType;
                DataType type = new DataType.Collection.Map(rawKeyType, rawValueType);
                Map value = (Map)getFixedValue(type);;
                PreparedStatement ps = session.prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_map', ?)", ALL_MAP_TABLE, name));
                BoundStatement bs = ps.newBoundStatement();
                session.execute(setBoundValue(bs, name, type, value));

                CQLRow row = session.execute(String.format("SELECT %s FROM %s WHERE k='prepared_map'", name, ALL_MAP_TABLE)).fetchOne();
                assertEquals("For type " + type, value, getValue(row, name, type));
            }
        }
    }

    //@Test
    //public void prepareAppendListTest() throws NoHostAvailableException {
    //    PreparedStatement ps = session.prepare(String.format("UPDATE %s SET c_list_int = c_list_int + ? WHERE k = 'prepare_append_list'", ALL_LIST_TABLE));
    //    BoundStatement bs = ps.newBoundStatement();
    //    session.execute(bs.setList("c_list_int", Arrays.asList(1, 2, 3)));
    //}
}
