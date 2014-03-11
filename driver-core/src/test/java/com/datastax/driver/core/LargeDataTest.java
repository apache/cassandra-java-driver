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

import java.nio.ByteBuffer;

import static com.datastax.driver.core.TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;

/**
 * Test limitations when using large amounts of data with the driver
 */
public class LargeDataTest {

    /*
     * Test a wide row of size 1,000,000
     * @param c The cluster object
     * @param key The key value that will receive the data
     * @throws Throwable
     */
    private void testWideRows(CCMBridge.CCMCluster c, int key) throws Throwable {
        // Write data
        for (int i = 0; i < 1000000; ++i) {
            c.session.execute(insertInto("wide_rows").value("k", key).value("i", i).setConsistencyLevel(ConsistencyLevel.QUORUM));
        }

        // Read data
        ResultSet rs = c.session.execute(select("i").from("wide_rows").where(eq("k", key)));

        // Verify data
        int i = 0;
        for (Row row : rs) {
            assertTrue(row.getInt("i") == i++);
        }
    }

    /*
     * Test a batch that writes a row of size 10,000
     * @param c The cluster object
     * @param key The key value that will receive the data
     * @throws Throwable
     */
    private void testWideBatchRows(CCMBridge.CCMCluster c, int key) throws Throwable {
        // Write data
        Batch q = batch();
        for (int i = 0; i < 10000; ++i) {
            q = q.add(insertInto("wide_batch_rows").value("k", key).value("i", i));
        }
        c.session.execute(q.setConsistencyLevel(ConsistencyLevel.QUORUM));

        // Read data
        ResultSet rs = c.session.execute(select("i").from("wide_batch_rows").where(eq("k", key)));

        // Verify data
        int i = 0;
        for (Row row : rs) {
            assertTrue(row.getInt("i") == i++);
        }
    }

    /*
     * Test a wide row of size 1,000,000 consisting of a ByteBuffer
     * @param c The cluster object
     * @param key The key value that will receive the data
     * @throws Throwable
     */
    private void testByteRows(CCMBridge.CCMCluster c, int key) throws Throwable {
        // Build small ByteBuffer sample
        ByteBuffer bb = ByteBuffer.allocate(58);
        bb.putShort((short) 0xCAFE);
        bb.flip();

        // Write data
        for (int i = 0; i < 1000000; ++i) {
            c.session.execute(insertInto("wide_byte_rows").value("k", key).value("i", bb).setConsistencyLevel(ConsistencyLevel.QUORUM));
        }

        // Read data
        ResultSet rs = c.session.execute(select("i").from("wide_byte_rows").where(eq("k", key)));

        // Verify data
        int i = 0;
        for (Row row : rs) {
            assertEquals(row.getBytes("i"), bb);
        }
    }

    /*
     * Test a row with a single extra large text value
     * @param c The cluster object
     * @param key The key value that will receive the data
     * @throws Throwable
     */
    private void testLargeText(CCMBridge.CCMCluster c, int key) throws Throwable {
        // Write data
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 1000000; ++i) {
            // Create ultra-long text
            b.append(i);
        }
        c.session.execute(insertInto("large_text").value("k", key).value("txt", b.toString()).setConsistencyLevel(ConsistencyLevel.QUORUM));

        // Read data
        Row row = c.session.execute(select().all().from("large_text").where(eq("k", key))).one();

        // Verify data
        assertTrue(b.toString().equals(row.getString("txt")));
    }

    /*
     * Converts an integer to an string of letters
     * @param i The integer that maps to letter
     * @return
     */
    private static String createColumnName(int i) {
        String[] letters = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};

        StringBuilder columnName;
        int currentI;

        currentI = i;
        columnName = new StringBuilder();
        while (true) {
            columnName.append(letters[currentI % 10]);
            currentI /= 10;
            if (currentI == 0)
                break;
        }

        return columnName.toString();
    }

    /*
     * Creates a table with 330 columns
     * @param c The cluster object
     * @param key The key value that will receive the data
     * @throws Throwable
     */
    private void testWideTable(CCMBridge.CCMCluster c, int key) throws Throwable {
        // Write data
        Insert insertStatement = insertInto("wide_table").value("k", key);
        for (int i = 0; i < 330; ++i) {
            insertStatement = insertStatement.value(createColumnName(i), i);
        }
        c.session.execute(insertStatement.setConsistencyLevel(ConsistencyLevel.QUORUM));

        // Read data
        Row row = c.session.execute(select().all().from("wide_table").where(eq("k", key))).one();

        // Verify data
        for (int i = 0; i < 330; ++i) {
            assertTrue(row.getInt(createColumnName(i)) == i);
        }
    }


    /**
     * Test a wide row of size 1,000,000
     * @throws Throwable
     */
    @Test(groups = "integration")
    public void wideRows() throws Throwable {
        Cluster.Builder builder = Cluster.builder();
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(1, builder);

        c.session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "large_data", 1));
        c.session.execute("USE large_data");
        c.session.execute(String.format("CREATE TABLE %s (k INT, i INT, PRIMARY KEY(k, i))", "wide_rows"));

        try {
            testWideRows(c, 0);
        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            c.discard();
        }
    }

    /**
     * Test a batch that writes a row of size 10,000
     * @throws Throwable
     */
    @Test(groups = "integration")
    public void wideBatchRows() throws Throwable {
        Cluster.Builder builder = Cluster.builder();
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(1, builder);

        c.session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "large_data", 1));
        c.session.execute("USE large_data");
        c.session.execute(String.format("CREATE TABLE %s (k INT, i INT, PRIMARY KEY(k, i))", "wide_batch_rows"));

        try {
            testWideBatchRows(c, 0);
        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            c.discard();
        }
    }

    /**
     * Test a wide row of size 1,000,000 consisting of a ByteBuffer
     * @throws Throwable
     */
    @Test(groups = "integration")
    public void wideByteRows() throws Throwable {
        Cluster.Builder builder = Cluster.builder();
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(1, builder);

        c.session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "large_data", 1));
        c.session.execute("USE large_data");
        c.session.execute(String.format("CREATE TABLE %s (k INT, i BLOB, PRIMARY KEY(k, i))", "wide_byte_rows"));

        try {
            testByteRows(c, 0);
        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            c.discard();
        }
    }

    /**
     * Test a row with a single extra large text value
     * @throws Throwable
     */
    @Test(groups = "integration")
    public void largeText() throws Throwable {
        Cluster.Builder builder = Cluster.builder();
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(1, builder);

        c.session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "large_data", 1));
        c.session.execute("USE large_data");
        c.session.execute(String.format("CREATE TABLE %s (k int PRIMARY KEY, txt text)", "large_text"));

        try {
            testLargeText(c, 0);
        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            c.discard();
        }
    }

    /**
     * Creates a table with 330 columns
     * @throws Throwable
     */
    @Test(groups = "integration")
    public void wideTable() throws Throwable {
        Cluster.Builder builder = Cluster.builder();
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(1, builder);

        c.session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "large_data", 1));
        c.session.execute("USE large_data");

        // Create the extra wide table definition
        StringBuilder tableDeclaration = new StringBuilder();
        tableDeclaration.append("CREATE TABLE wide_table (");
        tableDeclaration.append("k INT PRIMARY KEY");
        for (int i = 0; i < 330; ++i) {
            tableDeclaration.append(String.format(", %s INT", createColumnName(i)));
        }
        tableDeclaration.append(")");
        c.session.execute(tableDeclaration.toString());

        try {
            testWideTable(c, 0);
        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            c.discard();
        }
    }

    /**
     * Tests 10 random tests consisting of the other methods in this class
     * @throws Throwable
     */
    @Test(groups = "duration")
    public void mixedDurationTest() throws Throwable {
        Cluster.Builder builder = Cluster.builder();
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(3, builder);

        c.session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "large_data", 3));
        c.session.execute("USE large_data");
        c.session.execute(String.format("CREATE TABLE %s (k INT, i INT, PRIMARY KEY(k, i))", "wide_rows"));
        c.session.execute(String.format("CREATE TABLE %s (k INT, i INT, PRIMARY KEY(k, i))", "wide_batch_rows"));
        c.session.execute(String.format("CREATE TABLE %s (k INT, i BLOB, PRIMARY KEY(k, i))", "wide_byte_rows"));
        c.session.execute(String.format("CREATE TABLE %s (k int PRIMARY KEY, txt text)", "large_text"));

        // Create the extra wide table definition
        StringBuilder tableDeclaration = new StringBuilder();
        tableDeclaration.append("CREATE TABLE wide_table (");
        tableDeclaration.append("k INT PRIMARY KEY");
        for (int i = 0; i < 330; ++i) {
            tableDeclaration.append(String.format(", %s INT", createColumnName(i)));
        }
        tableDeclaration.append(")");
        c.session.execute(tableDeclaration.toString());

        try {
            for (int i = 0; i < 10; ++i) {
                switch ((int) (Math.random() * 5)){
                    case 0: testWideRows(c, 0); break;
                    case 1: testWideBatchRows(c, 0); break;
                    case 2: testByteRows(c, 0); break;
                    case 3: testLargeText(c, 0); break;
                    case 4: testWideTable(c, 0); break;
                    default: break;
                }
            }
        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            c.discard();
        }
    }
}
