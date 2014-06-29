package com.datastax.driver.core;

import java.util.*;

import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;
import static com.datastax.driver.core.TestUtils.versionCheck;

public class TupleTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        versionCheck(2.1, 0, "This will only work with Cassandra 2.1.0");

        return Arrays.asList("CREATE TABLE t (k int PRIMARY KEY, v tuple<int, text, float>)");
    }

    @Test(groups = "short")
    public void simpleValueTest() throws Exception {
        TupleValue v = new TupleValue(DataType.cint(), DataType.text(), DataType.cfloat());
        v.setInt(0, 1);
        v.setString(1, "a");
        v.setFloat(2, 1.0f);

        assertEquals(v.getTypes().size(), 3);
        assertEquals(v.getTypes().get(0), DataType.cint());
        assertEquals(v.getTypes().get(1), DataType.text());
        assertEquals(v.getTypes().get(2), DataType.cfloat());

        assertEquals(v.getInt(0), 1);
        assertEquals(v.getString(1), "a");
        assertEquals(v.getFloat(2), 1.0f);

        DataType type = DataType.tupleType(v.getTypes());
        assertEquals(type.format(v), "(1, 'a', 1.0)");
    }

    @Test(groups = "short")
    public void simpleWriteReadTest() throws Exception {
        try {
            PreparedStatement ins = session.prepare("INSERT INTO t(k, v) VALUES (?, ?)");
            PreparedStatement sel = session.prepare("SELECT * FROM t WHERE k=?");

            int k = 1;
            TupleValue v = new TupleValue(DataType.cint(), DataType.text(), DataType.cfloat());
            v.setInt(0, 1);
            v.setString(1, "a");
            v.setFloat(2, 1.0f);

            session.execute(ins.bind(k, v));
            TupleValue v2 = session.execute(sel.bind(k)).one().getTupleValue("v");

            assertEquals(v2, v);

            // Test simple statement interpolation
            k = 2;
            v = new TupleValue(DataType.cint(), DataType.text(), DataType.cfloat());
            v.setInt(0, 2);
            v.setString(1, "b");
            v.setFloat(2, 2.0f);

            session.execute("INSERT INTO t(k, v) VALUES (?, ?)", k, v);
            v2 = session.execute(sel.bind(k)).one().getTupleValue("v");

            assertEquals(v2, v);
        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }
}
