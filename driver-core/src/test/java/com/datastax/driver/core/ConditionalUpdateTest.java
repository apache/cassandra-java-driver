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
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Test {@link ResultSet#wasApplied()} for conditional updates.
 */
@CassandraVersion(value = "2.0.0", description = "Conditional Updates requires 2.0+.")
public class ConditionalUpdateTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE test(k1 int, k2 int, v int, PRIMARY KEY (k1, k2))");
    }

    @Test(groups = "short")
    public void singleUpdateTest() {
        session().execute("TRUNCATE test");
        session().execute("INSERT INTO test (k1, k2, v) VALUES (1, 1, 1)");

        ResultSet rs = session().execute("UPDATE test SET v = 3 WHERE k1 = 1 AND k2 = 1 IF v = 2");
        assertFalse(rs.wasApplied());
        // Ensure that reading the status does not consume a row:
        assertFalse(rs.isExhausted());

        rs = session().execute("UPDATE test SET v = 3 WHERE k1 = 1 AND k2 = 1 IF v = 1");
        assertTrue(rs.wasApplied());
        assertFalse(rs.isExhausted());

        // Non-conditional statement
        rs = session().execute("UPDATE test SET v = 4 WHERE k1 = 1 AND k2 = 1");
        assertTrue(rs.wasApplied());
    }

    @Test(groups = "short")
    public void batchUpdateTest() {
        session().execute("TRUNCATE test");
        session().execute("INSERT INTO test (k1, k2, v) VALUES (1, 1, 1)");
        session().execute("INSERT INTO test (k1, k2, v) VALUES (1, 2, 1)");

        PreparedStatement ps = session().prepare("UPDATE test SET v = :new WHERE k1 = :k1 AND k2 = :k2 IF v = :old");
        BatchStatement batch = new BatchStatement();
        batch.add(ps.bind().setInt("k1", 1).setInt("k2", 1).setInt("old", 2).setInt("new", 3)); // will fail
        batch.add(ps.bind().setInt("k1", 1).setInt("k2", 2).setInt("old", 1).setInt("new", 3));


        ResultSet rs = session().execute(batch);
        assertFalse(rs.wasApplied());
    }


    @Test(groups = "short")
    public void multipageResultSetTest() {
        session().execute("TRUNCATE test");
        session().execute("INSERT INTO test (k1, k2, v) VALUES (1, 1, 1)");
        session().execute("INSERT INTO test (k1, k2, v) VALUES (1, 2, 1)");

        // This is really contrived, we just want to cover the code path in ArrayBackedResultSet#MultiPage.
        // Currently CAS update results are never multipage, so it's hard to come up with a meaningful example.
        ResultSet rs = session().execute(new SimpleStatement("SELECT * FROM test WHERE k1 = 1").setFetchSize(1));

        assertTrue(rs.wasApplied());
    }

    /**
     * Test for #JAVA-358 - Directly expose CAS_RESULT_COLUMN.
     * <p/>
     * This test makes sure that the boolean flag {@code ResultSet.wasApplied()} is false when we try to insert a row
     * which already exists.
     *
     * @see ResultSet#wasApplied()
     */
    @Test(groups = "short")
    public void insert_if_not_exist_should_support_wasApplied_boolean() {
        // First, make sure the test table and the row exist
        session().execute("CREATE TABLE IF NOT EXISTS Java358 (key int primary key, value int)");
        ResultSet rs;
        rs = session().execute("INSERT INTO Java358(key, value) VALUES (42, 42) IF NOT EXISTS");
        assertTrue(rs.wasApplied());

        // Then, make sure the flag reports correctly that we did not create a new row
        rs = session().execute("INSERT INTO Java358(key, value) VALUES (42, 42) IF NOT EXISTS");
        assertFalse(rs.wasApplied());
    }

    /**
     * Test for #JAVA-358 - Directly expose CAS_RESULT_COLUMN.
     * <p/>
     * This test makes sure that the boolean flag {@code ResultSet.wasApplied()} is false when we try to delete a row
     * which does not exist.
     *
     * @see ResultSet#wasApplied()
     */
    @Test(groups = "short")
    public void delete_if_not_exist_should_support_wasApplied_boolean() {
        // First, make sure the test table and the row exist
        session().execute("CREATE TABLE IF NOT EXISTS Java358 (key int primary key, value int)");
        session().execute("INSERT INTO Java358(key, value) VALUES (42, 42)");

        // Then, make sure the flag reports correctly that we did delete the row
        ResultSet rs;
        rs = session().execute("DELETE FROM Java358 WHERE KEY=42 IF EXISTS");
        assertTrue(rs.wasApplied());

        // Finally, make sure the flag reports correctly that we did did not delete an non-existing row
        rs = session().execute("DELETE FROM Java358 WHERE KEY=42 IF EXISTS");
        assertFalse(rs.wasApplied());
    }
}
