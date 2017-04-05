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

import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@CreateCCM(PER_METHOD)
public class BatchStatementTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE test (k text, v int, PRIMARY KEY (k, v))");
    }

    @Test(groups = "short")
    public void simpleBatchTest() {
        try {
            PreparedStatement st = session().prepare("INSERT INTO test (k, v) VALUES (?, ?)");

            BatchStatement batch = new BatchStatement();

            batch.add(new SimpleStatement("INSERT INTO test (k, v) VALUES (?, ?)", "key1", 0));
            batch.add(st.bind("key1", 1));
            batch.add(st.bind("key2", 0));

            assertEquals(3, batch.size());

            session().execute(batch);

            ResultSet rs = session().execute("SELECT * FROM test");

            Row r;

            r = rs.one();
            assertEquals(r.getString("k"), "key1");
            assertEquals(r.getInt("v"), 0);

            r = rs.one();
            assertEquals(r.getString("k"), "key1");
            assertEquals(r.getInt("v"), 1);

            r = rs.one();
            assertEquals(r.getString("k"), "key2");
            assertEquals(r.getInt("v"), 0);

            assertTrue(rs.isExhausted());

        } catch (UnsupportedFeatureException e) {
            // This is expected when testing the protocol v1
            assertEquals(cluster().getConfiguration().getProtocolOptions().getProtocolVersion(), ProtocolVersion.V1);
        }
    }

    @Test(groups = "short")
    @CassandraVersion(value = "2.0.9", description = "This will only work with C* 2.0.9 (CASSANDRA-7337)")
    public void casBatchTest() {
        PreparedStatement st = session().prepare("INSERT INTO test (k, v) VALUES (?, ?) IF NOT EXISTS");

        BatchStatement batch = new BatchStatement();

        batch.add(new SimpleStatement("INSERT INTO test (k, v) VALUES (?, ?)", "key1", 0));
        batch.add(st.bind("key1", 1));
        batch.add(st.bind("key1", 2));

        assertEquals(3, batch.size());

        ResultSet rs = session().execute(batch);
        Row r = rs.one();
        assertTrue(!r.isNull("[applied]"));
        assertEquals(r.getBool("[applied]"), true);

        rs = session().execute(batch);
        r = rs.one();
        assertTrue(!r.isNull("[applied]"));
        assertEquals(r.getBool("[applied]"), false);
    }
}
