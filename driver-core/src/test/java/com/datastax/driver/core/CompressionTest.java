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

import java.util.Locale;

import static com.datastax.driver.core.SessionTest.checkExecuteResultSet;
import static org.assertj.core.api.Assertions.assertThat;

public class CompressionTest extends CCMTestsSupport {

    private static String TABLE = "test";

    public void onTestContextInitialized() {
        execute(String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", TABLE));
    }

    void compressionTest(ProtocolOptions.Compression compression) {
        cluster().getConfiguration().getProtocolOptions().setCompression(compression);
        try {
            Session compressedSession = cluster().connect(keyspace);

            // Simple calls to all versions of the execute/executeAsync methods
            String key = "execute_compressed_test_" + compression;
            ResultSet rs = compressedSession.execute(String.format(Locale.US, "INSERT INTO %s (k, t, i, f) VALUES ('%s', '%s', %d, %f)", TABLE, key, "foo", 42, 24.03f));
            assertThat(rs.isExhausted()).isTrue();

            String SELECT_ALL = String.format(TestUtils.SELECT_ALL_FORMAT + " WHERE k = '%s'", TABLE, key);

            // execute
            checkExecuteResultSet(compressedSession.execute(SELECT_ALL), key);
            checkExecuteResultSet(compressedSession.execute(new SimpleStatement(SELECT_ALL).setConsistencyLevel(ConsistencyLevel.ONE)), key);

            // executeAsync
            checkExecuteResultSet(compressedSession.executeAsync(SELECT_ALL).getUninterruptibly(), key);
            checkExecuteResultSet(compressedSession.executeAsync(new SimpleStatement(SELECT_ALL).setConsistencyLevel(ConsistencyLevel.ONE)).getUninterruptibly(), key);

        } finally {
            cluster().getConfiguration().getProtocolOptions().setCompression(ProtocolOptions.Compression.NONE);
        }
    }
}
