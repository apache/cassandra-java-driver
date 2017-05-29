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

public class DirectCompressionTest extends CompressionTest {

    /**
     * Validates that a session can be established using snappy compression and executes some queries that inserts and
     * retrieves data using that session().
     *
     * @test_category connection:compression
     * @expected_result session established and queries made successfully using it.
     */
    @Test(groups = "short")
    public void should_function_with_snappy_compression() throws Exception {
        compressionTest(ProtocolOptions.Compression.SNAPPY);
    }

    /**
     * Validates that a session can be established using lz4 compression and executes some queries that inserts and
     * retrieves data using that session().
     *
     * @test_category connection:compression
     * @expected_result session established and queries made successfully using it.
     */
    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_function_with_lz4_compression() throws Exception {
        compressionTest(ProtocolOptions.Compression.LZ4);
    }
}
