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
    private static final String COUNTER_TABLE = "counters";

    private static final String INSERT_FORMAT = "INSERT INTO %s (k, t, i, f) VALUES ('%s', '%s', %d, %f)";
    private static final String SELECT_ALL_FORMAT = "SELECT * FROM %s";

    protected Collection<String> getTableDefinitions() {
        return Arrays.asList(String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", TABLE),
                             String.format("CREATE TABLE %s (k text PRIMARY KEY, c counter)", COUNTER_TABLE));
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
        Row row = rs.one();
        assertTrue(rs.isExhausted());
        assertEquals(key,    row.getString("k"));
        assertEquals("foo",  row.getString("t"));
        assertEquals(42,     row.getInt("i"));
        assertEquals(24.03f, row.getFloat("f"), 0.1f);
    }

    @Test
    public void setAndDropKeyspaceTest() throws Exception {
        // Check that if someone set a keyspace and then drop it, we recognize
        // that fact and don't assume he is still set to this keyspace

        try {
            session.execute(String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, "to_drop", 1));
            session.execute("USE to_drop");
            session.execute("DROP KEYSPACE to_drop");

            assertEquals(null, session.manager.poolsState.keyspace);
        } finally {
            // restore the correct state for remaining states
            session.execute("USE " + TestUtils.SIMPLE_KEYSPACE);
        }
    }

    @Test
    public void executePreparedCounterTest() throws Exception {
        PreparedStatement p = session.prepare("UPDATE " + COUNTER_TABLE + " SET c = c + ? WHERE k = ?");

        session.execute(p.bind(1L, "row"));
        session.execute(p.bind(1L, "row"));

        ResultSet rs = session.execute("SELECT * FROM " + COUNTER_TABLE);
        List<Row> rows = rs.all();
        assertEquals(1, rows.size());
        assertEquals(2L, rows.get(0).getLong("c"));
    }

    @Test
    public void compressionTest() throws Exception {

        // Same as executeTest, but with compression enabled

        cluster.getConfiguration().getProtocolOptions().setCompression(ProtocolOptions.Compression.SNAPPY);

        try {

            Session compressedSession = cluster.connect(TestUtils.SIMPLE_KEYSPACE);

            // Simple calls to all versions of the execute/executeAsync methods
            String key = "execute_compressed_test";
            ResultSet rs = compressedSession.execute(String.format(INSERT_FORMAT, TABLE, key, "foo", 42, 24.03f));
            assertTrue(rs.isExhausted());

            String SELECT_ALL = String.format(SELECT_ALL_FORMAT + " WHERE k = '%s'", TABLE, key);

            // execute
            checkExecuteResultSet(compressedSession.execute(SELECT_ALL), key);
            checkExecuteResultSet(compressedSession.execute(new SimpleStatement(SELECT_ALL).setConsistencyLevel(ConsistencyLevel.ONE)), key);

            // executeAsync
            checkExecuteResultSet(compressedSession.executeAsync(SELECT_ALL).getUninterruptibly(), key);
            checkExecuteResultSet(compressedSession.executeAsync(new SimpleStatement(SELECT_ALL).setConsistencyLevel(ConsistencyLevel.ONE)).getUninterruptibly(), key);

        } finally {
            cluster.getConfiguration().getProtocolOptions().setCompression(ProtocolOptions.Compression.NONE);
        }
    }

}
