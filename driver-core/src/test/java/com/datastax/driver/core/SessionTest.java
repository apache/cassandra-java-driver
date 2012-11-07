package com.datastax.driver.core;

import java.util.*;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import com.datastax.driver.core.exceptions.*;
import static com.datastax.driver.core.TestUtils.*;

/**
 * Simple test of the Sessions methods against a one node cluster.
 */
public class SessionTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final String TABLE = "test";

    private static final String INSERT_FORMAT = "INSERT INTO %s (k, t, i, f) VALUES ('%s', '%s', %d, %f)";
    private static final String SELECT_ALL_FORMAT = "SELECT * FROM %s";

    protected Collection<String> getTableDefinitions() {
        return Collections.singleton(String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", TABLE));
    }

    @Test
    public void executeTest() throws Exception {
        // Simple calls to all versions of the execute/executeAsync methods
        String key = "execute_test";
        ResultSet rs = session.execute(String.format(INSERT_FORMAT, TABLE, key, "foo", 42, 24.03f));
        assertTrue(rs.isExhausted());

        // execute
        checkExecuteResultSet(session.execute(String.format(SELECT_ALL_FORMAT, TABLE)), key);
        checkExecuteResultSet(session.execute(String.format(SELECT_ALL_FORMAT, TABLE), ConsistencyLevel.ONE), key);
        checkExecuteResultSet(session.execute(String.format(SELECT_ALL_FORMAT, TABLE), new QueryOptions(ConsistencyLevel.ONE)), key);

        // executeAsync
        checkExecuteResultSet(session.executeAsync(String.format(SELECT_ALL_FORMAT, TABLE)).getUninterruptibly(), key);
        checkExecuteResultSet(session.executeAsync(String.format(SELECT_ALL_FORMAT, TABLE), ConsistencyLevel.ONE).getUninterruptibly(), key);
        checkExecuteResultSet(session.executeAsync(String.format(SELECT_ALL_FORMAT, TABLE), new QueryOptions(ConsistencyLevel.ONE)).getUninterruptibly(), key);
    }

    @Test
    public void executePreparedTest() throws Exception {
        // Simple calls to all versions of the executePrepared/executePreparedAsync methods
        String key = "execute_prepared_test";
        ResultSet rs = session.execute(String.format(INSERT_FORMAT, TABLE, key, "foo", 42, 24.03f));
        assertTrue(rs.isExhausted());

        PreparedStatement p = session.prepare(String.format(SELECT_ALL_FORMAT + " WHERE k = ?", TABLE));
        BoundStatement bs = p.bind(key);

        // executePrepared
        checkExecuteResultSet(session.executePrepared(bs), key);
        checkExecuteResultSet(session.executePrepared(bs, ConsistencyLevel.ONE), key);
        checkExecuteResultSet(session.executePrepared(bs, new QueryOptions(ConsistencyLevel.ONE)), key);

        // executePreparedAsync
        checkExecuteResultSet(session.executePreparedAsync(bs).getUninterruptibly(), key);
        checkExecuteResultSet(session.executePreparedAsync(bs, ConsistencyLevel.ONE).getUninterruptibly(), key);
        checkExecuteResultSet(session.executePreparedAsync(bs, new QueryOptions(ConsistencyLevel.ONE)).getUninterruptibly(), key);
    }

    private static void checkExecuteResultSet(ResultSet rs, String key) {
        assertTrue(!rs.isExhausted());
        CQLRow row = rs.fetchOne();
        assertTrue(rs.isExhausted());
        assertEquals(key,    row.getString("k"));
        assertEquals("foo",  row.getString("t"));
        assertEquals(42,     row.getInt("i"));
        assertEquals(24.03f, row.getFloat("f"), 0.1f);
    }
}
