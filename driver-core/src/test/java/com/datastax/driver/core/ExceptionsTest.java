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

import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;

import com.datastax.driver.core.exceptions.*;

/**
 * Simple test of the Exception classes against a one node cluster.
 */
public class ExceptionsTest extends CCMBridge.PerClassSingleNodeCluster {

    protected Collection<String> getTableDefinitions() {
        return Arrays.asList("CREATE TABLE null (k text PRIMARY KEY, t text, i int, f float)");
    }

    @Test
    public void alreadyExistsException() throws Exception {
        Session aeeSession = cluster.connect();
        String aeeKeyspace = "AEESchemaKeyspace";
        String aeeTable = "AEESchemaTable";

        String[] cqlCommands = new String[]{
            String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, aeeKeyspace, 1),
            "USE " + aeeKeyspace,
            String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", aeeTable)
        };

        // Create the schema once
        aeeSession.execute(cqlCommands[0]);
        aeeSession.execute(cqlCommands[1]);
        aeeSession.execute(cqlCommands[2]);

        // Try creating the keyspace again
        try {
            aeeSession.execute(cqlCommands[0]);
        } catch (AlreadyExistsException e) {
            String expected = String.format("Keyspace %s already exists", aeeKeyspace.toLowerCase());
            assertEquals(expected, e.getMessage());
            assertEquals(aeeKeyspace.toLowerCase(), e.getKeyspace());
            assertEquals(null, e.getTable());
            assertEquals(false, e.wasTableCreation());
        }

        aeeSession.execute(cqlCommands[1]);

        // Try creating the table again
        try {
            aeeSession.execute(cqlCommands[2]);
        } catch (AlreadyExistsException e) {
            String expected = String.format("Table %s.%s already exists", aeeKeyspace.toLowerCase(), aeeTable.toLowerCase());
            assertEquals(expected, e.getMessage());
            assertEquals(aeeKeyspace.toLowerCase(), e.getKeyspace());
            assertEquals(aeeTable.toLowerCase(), e.getTable());
            assertEquals(true, e.wasTableCreation());
        }
    }
}
