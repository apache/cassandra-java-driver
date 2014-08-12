/*
 *      Copyright (C) 2014 DataStax Inc.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.datastax.driver.core.exceptions.NoHostAvailableException;

import static com.datastax.driver.core.TestUtils.*;
import static org.testng.Assert.fail;

public class NodeTest extends AbstractPoliciesTest {

    private static final Logger logger = LoggerFactory.getLogger(CCMBridge.class);

    /**
     * Test to ensure that a fully downed cluster is recognized after being restarted.
     * Test for JAVA-367
     *
     * @throws Exception
     */
    @Test(groups = "long")
    public void nodeDownUpTest() throws Exception {
        // create a new cluster object
        Cluster.Builder builder = Cluster.builder();
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(1, builder);

        try {
            // ensure the node is well-formed
            createSchema(c.session, 1);
            init(c, 0, ConsistencyLevel.ONE);
            query(c, 0, ConsistencyLevel.ONE);
            assertQueried(CCMBridge.IP_PREFIX + '1', 0);

            // shut down the cluster
            c.cassandraCluster.stop();
            waitForDown(CCMBridge.IP_PREFIX + '1', c.cluster);

            // ensure the cluster is down
            try {
                init(c, 0, ConsistencyLevel.ONE);
                c.errorOut();
                fail("Node is not down; expected node to be down");
            } catch (NoHostAvailableException e) {
                logger.info("Caught expected exception: NoHostAvailableException");
            }

            // bring up the cluster
            c.cassandraCluster.start();
            waitFor(CCMBridge.IP_PREFIX + '1', c.cluster, 120);

            // retry query with same session
            init(c, 0, ConsistencyLevel.ONE);
            query(c, 0, ConsistencyLevel.ONE);
            assertQueried(CCMBridge.IP_PREFIX + '1', 0);

        } catch (Exception e) {
            c.errorOut();
            throw e;
        }
    }
}