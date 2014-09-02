/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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
import java.util.Set;

import org.testng.annotations.*;

import static org.testng.Assert.assertFalse;

import com.datastax.driver.core.CCMBridge.CCMCluster;

/**
 * Tests that the token map is correctly initialized at startup (JAVA-415).
 */
public class TokenMapTest {
    CCMCluster ccmCluster = null;
    Cluster cluster = null;

    @BeforeMethod(groups = "short")
    public void setup() {
        ccmCluster = CCMBridge.buildCluster(1, Cluster.builder());
        cluster = ccmCluster.cluster;
    }

    @Test(groups = "short")
    public void initTest() {
        // If the token map is initialized correctly, we should get replicas for any partition key
        ByteBuffer anyKey = ByteBuffer.wrap(new byte[]{});
        Set<Host> replicas = cluster.getMetadata().getReplicas("system", anyKey);
        assertFalse(replicas.isEmpty());
    }

    @AfterMethod(groups = "short")
    public void teardown() {
        ccmCluster.discard();
    }
}
