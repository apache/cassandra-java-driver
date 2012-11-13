package com.datastax.driver.core;

import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;

import com.datastax.driver.core.exceptions.*;

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
        checkExecuteResultSet(session.execute(new SimpleStatement(String.format(SELECT_ALL_FORMAT, TABLE)).setConsistencyLevel(ConsistencyLevel.ONE)), key);

        // executeAsync
        checkExecuteResultSet(session.executeAsync(String.format(SELECT_ALL_FORMAT, TABLE)).getUninterruptibly(), key);
        checkExecuteResultSet(session.executeAsync(new SimpleStatement(String.format(SELECT_ALL_FORMAT, TABLE)).setConsistencyLevel(ConsistencyLevel.ONE)).getUninterruptibly(), key);
    }

    @Test
    public void executePreparedTest() throws Exception {
        // Simple calls to all versions of the execute/executeAsync methods for prepared statements
        // Note: the goal is only to exercice the Session methods, PreparedStatementTest have better prepared statement tests.
        String key = "execute_prepared_test";
        ResultSet rs = session.execute(String.format(INSERT_FORMAT, TABLE, key, "foo", 42, 24.03f));
        assertTrue(rs.isExhausted());

        PreparedStatement p = session.prepare(String.format(SELECT_ALL_FORMAT + " WHERE k = ?", TABLE));
        BoundStatement bs = p.bind(key);

        // executePrepared
        checkExecuteResultSet(session.execute(bs), key);
        checkExecuteResultSet(session.execute(bs.setConsistencyLevel(ConsistencyLevel.ONE)), key);

        // executePreparedAsync
        checkExecuteResultSet(session.executeAsync(bs).getUninterruptibly(), key);
        checkExecuteResultSet(session.executeAsync(bs.setConsistencyLevel(ConsistencyLevel.ONE)).getUninterruptibly(), key);
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

    @Test
    public void setAndDropKeyspace() throws Exception {
        // Check that if someone set a keyspace and then drop it, we recognize
        // that fact and don't assume he is still set to this keyspace

        session.execute(String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, "to_drop", 1));
        session.execute("USE to_drop");
        session.execute("DROP KEYSPACE to_drop");

        assertEquals(null, session.manager.poolsState.keyspace);
    }
}
