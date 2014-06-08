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

import java.util.Collection;
import java.util.Collections;

import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datastax.driver.core.exceptions.UnsupportedFeatureException;

public class BatchStatementTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Collections.singletonList("CREATE TABLE test (k text, v int, PRIMARY KEY (k, v))");
    }

    @Test(groups = "short")
    public void simpleBatchTest() throws Throwable {
        try {
            PreparedStatement st = session.prepare("INSERT INTO test (k, v) VALUES (?, ?)");

            BatchStatement batch = new BatchStatement();

            batch.add(new SimpleStatement("INSERT INTO test (k, v) VALUES (?, ?)", "key1", 0));
            batch.add(st.bind("key1", 1));
            batch.add(st.bind("key2", 0));

            session.execute(batch);

            ResultSet rs = session.execute("SELECT * FROM test");

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

            session.execute("DELETE FROM test WHERE k='key1'");
            session.execute("DELETE FROM test WHERE k='key2'");

        } catch (UnsupportedFeatureException e) {
            // This is expected when testing the protocol v1
            if (cluster.getConfiguration().getProtocolOptions().getProtocolVersion() != 1)
                throw e;
        } catch (Throwable t) {
            errorOut();
            throw t;
        }
    }

    @Test(groups = "short")
    public void casBatchTest() throws Throwable {
        // This will only work with C* 2.0.9 (CASSANDRA-7337)
        try {
            PreparedStatement st = session.prepare("INSERT INTO test (k, v) VALUES (?, ?) IF NOT EXISTS");

            BatchStatement batch = new BatchStatement();

            batch.add(new SimpleStatement("INSERT INTO test (k, v) VALUES (?, ?)", "key1", 0));
            batch.add(st.bind("key1", 1));
            batch.add(st.bind("key1", 2));

            ResultSet rs = session.execute(batch);
            Row r = rs.one();
            assertTrue(!r.isNull("[applied]"));
            assertEquals(r.getBool("[applied]"), true);

            rs = session.execute(batch);
            r = rs.one();
            assertTrue(!r.isNull("[applied]"));
            assertEquals(r.getBool("[applied]"), false);

            session.execute("DELETE FROM test WHERE k='key1'");

        } catch (UnsupportedFeatureException e) {
            // This is expected when testing the protocol v1
            if (cluster.getConfiguration().getProtocolOptions().getProtocolVersion() != 1)
                throw e;
        } catch (Throwable t) {
            errorOut();
            throw t;
        }
    }
}
