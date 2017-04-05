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

import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static com.datastax.driver.core.TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Test limitations when using large amounts of data with the driver
 */
@CreateCCM(PER_METHOD)
public class LargeDataTest extends CCMTestsSupport {

    /*
     * Test a wide row of size 1,000,000
     * @param c The cluster object
     * @param key The key value that will receive the data
     * @throws Throwable
     */
    private void testWideRows(int key) throws Throwable {
        // Write data
        for (int i = 0; i < 1000000; ++i) {
            session().execute(insertInto("wide_rows").value("k", key).value("i", i).setConsistencyLevel(ConsistencyLevel.QUORUM));
        }

        // Read data
        ResultSet rs = session().execute(select("i").from("wide_rows").where(eq("k", key)));

        // Verify data
        int i = 0;
        for (Row row : rs) {
            assertTrue(row.getInt("i") == i++);
        }
    }

    /*
     * Test a batch that writes a row of size 4,000
     * @param c The cluster object
     * @param key The key value that will receive the data
     * @throws Throwable
     */
    private void testWideBatchRows(int key) throws Throwable {
        // Write data
        Batch q = batch();
        for (int i = 0; i < 4000; ++i) {
            q = q.add(insertInto("wide_batch_rows").value("k", key).value("i", i));
        }
        session().execute(q.setConsistencyLevel(ConsistencyLevel.QUORUM));

        // Read data
        ResultSet rs = session().execute(select("i").from("wide_batch_rows").where(eq("k", key)));

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
    private void testByteRows(int key) throws Throwable {
        // Build small ByteBuffer sample
        ByteBuffer bb = ByteBuffer.allocate(58);
        bb.putShort((short) 0xCAFE);
        bb.flip();

        // Write data
        for (int i = 0; i < 1000000; ++i) {
            session().execute(insertInto("wide_byte_rows").value("k", key).value("i", bb).setConsistencyLevel(ConsistencyLevel.QUORUM));
        }

        // Read data
        ResultSet rs = session().execute(select("i").from("wide_byte_rows").where(eq("k", key)));

        // Verify data
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
    private void testLargeText(int key) throws Throwable {
        // Write data
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 1000000; ++i) {
            // Create ultra-long text
            b.append(i);
        }
        session().execute(insertInto("large_text").value("k", key).value("txt", b.toString()).setConsistencyLevel(ConsistencyLevel.QUORUM));

        // Read data
        Row row = session().execute(select().all().from("large_text").where(eq("k", key))).one();

        // Verify data
        assertTrue(b.toString().equals(row.getString("txt")));
    }

    /*
     * Converts an integer to an string of letters
     * @param i The integer that maps to letter
     * @return
     *
     * TODO This doesn't offer protection from generating column names that are reserved keywords.  This does
     * work with CQL 3.0, but may break in future specifications.  Should fix this to ensure it does not.
     */
    private static String createColumnName(int i) {
        String[] letters = {"a", "b", "c", "d", "e", "f", "g", "h", "j", "l"};

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
    private void testWideTable(int key) throws Throwable {
        // Write data
        Insert insertStatement = insertInto("wide_table").value("k", key);
        for (int i = 0; i < 330; ++i) {
            insertStatement = insertStatement.value(createColumnName(i), i);
        }
        session().execute(insertStatement.setConsistencyLevel(ConsistencyLevel.QUORUM));

        // Read data
        Row row = session().execute(select().all().from("wide_table").where(eq("k", key))).one();

        // Verify data
        for (int i = 0; i < 330; ++i) {
            assertTrue(row.getInt(createColumnName(i)) == i);
        }
    }


    /**
     * Test a wide row of size 1,000,000
     */
    @Test(groups = "stress")
    @CassandraVersion(value = "2.0.0", description = "< 2.0 is skipped as 1.2 does not handle reading wide rows well.")
    public void wideRows() throws Throwable {
        session().execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "large_data", 1));
        session().execute("USE large_data");
        session().execute(String.format("CREATE TABLE %s (k INT, i INT, PRIMARY KEY(k, i))", "wide_rows"));
        testWideRows(0);
    }

    /**
     * Test a batch that writes a row of size 4,000 (just below the error threshold for 2.2).
     */
    @Test(groups = "stress")
    @CassandraVersion(value = "2.0.0", description = "< 2.0 is skipped as 1.2 does not handle large batches well.")
    public void wideBatchRows() throws Throwable {
        session().execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "large_data", 1));
        session().execute("USE large_data");
        session().execute(String.format("CREATE TABLE %s (k INT, i INT, PRIMARY KEY(k, i))", "wide_batch_rows"));
        testWideBatchRows(0);
    }

    /**
     * Test a wide row of size 1,000,000 consisting of a ByteBuffer
     */
    @Test(groups = "stress")
    public void wideByteRows() throws Throwable {
        session().execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "large_data", 1));
        session().execute("USE large_data");
        session().execute(String.format("CREATE TABLE %s (k INT, i BLOB, PRIMARY KEY(k, i))", "wide_byte_rows"));
        testByteRows(0);
    }

    /**
     * Test a row with a single extra large text value
     */
    @Test(groups = "stress")
    public void largeText() throws Throwable {
        session().execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "large_data", 1));
        session().execute("USE large_data");
        session().execute(String.format("CREATE TABLE %s (k int PRIMARY KEY, txt text)", "large_text"));
        testLargeText(0);
    }

    /**
     * Creates a table with 330 columns
     */
    @Test(groups = "stress")
    public void wideTable() throws Throwable {
        session().execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "large_data", 1));
        session().execute("USE large_data");
        // Create the extra wide table definition
        StringBuilder tableDeclaration = new StringBuilder();
        tableDeclaration.append("CREATE TABLE wide_table (");
        tableDeclaration.append("k INT PRIMARY KEY");
        for (int i = 0; i < 330; ++i) {
            tableDeclaration.append(String.format(", %s INT", createColumnName(i)));
        }
        tableDeclaration.append(')');
        session().execute(tableDeclaration.toString());
        testWideTable(0);
    }

    /**
     * Tests 10 random tests consisting of the other methods in this class
     */
    @Test(groups = "duration")
    @CCMConfig(numberOfNodes = 3)
    public void mixedDurationTest() throws Throwable {
        session().execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "large_data", 3));
        session().execute("USE large_data");
        session().execute(String.format("CREATE TABLE %s (k INT, i INT, PRIMARY KEY(k, i))", "wide_rows"));
        session().execute(String.format("CREATE TABLE %s (k INT, i INT, PRIMARY KEY(k, i))", "wide_batch_rows"));
        session().execute(String.format("CREATE TABLE %s (k INT, i BLOB, PRIMARY KEY(k, i))", "wide_byte_rows"));
        session().execute(String.format("CREATE TABLE %s (k int PRIMARY KEY, txt text)", "large_text"));
        // Create the extra wide table definition
        StringBuilder tableDeclaration = new StringBuilder();
        tableDeclaration.append("CREATE TABLE wide_table (");
        tableDeclaration.append("k INT PRIMARY KEY");
        for (int i = 0; i < 330; ++i) {
            tableDeclaration.append(String.format(", %s INT", createColumnName(i)));
        }
        tableDeclaration.append(')');
        session().execute(tableDeclaration.toString());
        for (int i = 0; i < 10; ++i) {
            switch ((int) (Math.random() * 5)) {
                case 0:
                    testWideRows(0);
                    break;
                case 1:
                    testWideBatchRows(0);
                    break;
                case 2:
                    testByteRows(0);
                    break;
                case 3:
                    testLargeText(0);
                    break;
                case 4:
                    testWideTable(0);
                    break;
                default:
                    break;
            }
        }
    }
}
