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
import static org.testng.Assert.*;

/**
 * Test ResultSet paging correct behavior.
 */
public class FetchingTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Collections.singletonList(String.format("CREATE TABLE test (k text, v int, PRIMARY KEY (k, v))"));
    }

    @Test(groups = "short")
    public void simplePagingTest() throws Throwable {

        try {

            // Insert data
            String key = "paging_test";
            for (int i = 0; i < 100; i++)
                session.execute(String.format("INSERT INTO test (k, v) VALUES ('%s', %d)", key, i));

            SimpleStatement st = new SimpleStatement(String.format("SELECT v FROM test WHERE k='%s'", key));
            st.setFetchSize(5); // Ridiculously small fetch size for testing purpose. Don't do at home.
            ResultSet rs = session.execute(st);

            assertFalse(rs.isFullyFetched());

            for (int i = 0; i < 100; i++) {
                // isExhausted makes sure we do fetch if needed
                assertFalse(rs.isExhausted());
                assertEquals(rs.getAvailableWithoutFetching(), 5-(i%5));
                assertEquals(rs.one().getInt(0), i);
            }

            // BUG: CASSANDRA-6555
            assertTrue(rs.isExhausted());
            assertTrue(rs.isFullyFetched());

        } catch (Throwable e) {
            errorOut();
            throw e;
        }
    }
}
