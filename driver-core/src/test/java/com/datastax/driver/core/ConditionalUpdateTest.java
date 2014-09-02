package com.datastax.driver.core;

import java.util.Collection;
import java.util.Collections;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

/**
 * Test {@link ResultSet#wasApplied()} for conditional updates.
 */
public class ConditionalUpdateTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Collections.singletonList("CREATE TABLE test(k1 int, k2 int, v int, PRIMARY KEY (k1, k2))");
    }

    @Test(groups = "short")
    public void singleUpdateTest() {
        session.execute("TRUNCATE test");
        session.execute("INSERT INTO test (k1, k2, v) VALUES (1, 1, 1)");

        ResultSet rs = session.execute("UPDATE test SET v = 3 WHERE k1 = 1 AND k2 = 1 IF v = 2");
        assertFalse(rs.wasApplied());
        // Ensure that reading the status does not consume a row:
        assertFalse(rs.isExhausted());

        rs = session.execute("UPDATE test SET v = 3 WHERE k1 = 1 AND k2 = 1 IF v = 1");
        assertTrue(rs.wasApplied());
        assertFalse(rs.isExhausted());

        // Non-conditional statement
        rs = session.execute("UPDATE test SET v = 4 WHERE k1 = 1 AND k2 = 1");
        assertTrue(rs.wasApplied());
    }

    @Test(groups = "short")
    public void batchUpdateTest() {
        session.execute("TRUNCATE test");
        session.execute("INSERT INTO test (k1, k2, v) VALUES (1, 1, 1)");
        session.execute("INSERT INTO test (k1, k2, v) VALUES (1, 2, 1)");

        PreparedStatement ps = session.prepare("UPDATE test SET v = :new WHERE k1 = :k1 AND k2 = :k2 IF v = :old");
        BatchStatement batch = new BatchStatement();
        batch.add(ps.bind().setInt("k1", 1).setInt("k2", 1).setInt("old", 2).setInt("new", 3)); // will fail
        batch.add(ps.bind().setInt("k1", 1).setInt("k2", 2).setInt("old", 1).setInt("new", 3));


        ResultSet rs = session.execute(batch);
        assertFalse(rs.wasApplied());
    }


    @Test(groups = "short")
    public void multipageResultSetTest() {
        session.execute("TRUNCATE test");
        session.execute("INSERT INTO test (k1, k2, v) VALUES (1, 1, 1)");
        session.execute("INSERT INTO test (k1, k2, v) VALUES (1, 2, 1)");

        // This is really contrived, we just want to cover the code path in ArrayBackedResultSet#MultiPage.
        // Currently CAS update results are never multipage, so it's hard to come up with a meaningful example.
        ResultSet rs = session.execute(new SimpleStatement("SELECT * FROM test WHERE k1 = 1").setFetchSize(1));

        assertTrue(rs.wasApplied());
    }
}
