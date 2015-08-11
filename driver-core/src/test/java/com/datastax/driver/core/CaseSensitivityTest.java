/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import static com.datastax.driver.core.TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT;

public class CaseSensitivityTest {

    private void assertExists(CCMBridge.CCMCluster c, String fetchName, String realName) {
        KeyspaceMetadata km = c.cluster.getMetadata().getKeyspace(fetchName);
        assertNotNull(km);
        assertEquals(realName, km.getName());
    }

    private void assertNotExists(CCMBridge.CCMCluster c, String name) {
        assertNull(c.cluster.getMetadata().getKeyspace(name));
    }

    @Test(groups = "short")
    public void testCaseInsensitiveKeyspace() throws Throwable {
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(1, Cluster.builder());
        Session s = c.session;
        try {
            String ksName = "MyKeyspace";
            s.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, ksName, 1));

            assertExists(c, ksName, "mykeyspace");
            assertExists(c, "mykeyspace", "mykeyspace");
            assertExists(c, "MYKEYSPACE", "mykeyspace");

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            c.discard();
        }
    }

    @Test(groups = "short")
    public void testCaseSensitiveKeyspace() throws Throwable {
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(1, Cluster.builder());
        Session s = c.session;
        try {
            String ksName = "\"MyKeyspace\"";
            s.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, ksName, 1));

            assertExists(c, ksName, "MyKeyspace");
            assertExists(c, Metadata.quote("MyKeyspace"), "MyKeyspace");
            assertNotExists(c, "mykeyspace");
            assertNotExists(c, "MyKeyspace");
            assertNotExists(c, "MYKEYSPACE");

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            c.discard();
        }
    }
}
