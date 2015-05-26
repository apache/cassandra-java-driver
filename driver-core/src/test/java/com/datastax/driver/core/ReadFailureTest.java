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

import java.net.InetSocketAddress;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import com.datastax.driver.core.exceptions.ReadFailureException;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.datastax.driver.core.utils.CassandraVersion;

public class ReadFailureTest {

    @Test(groups = "short", expectedExceptions = ReadFailureException.class)
    @CassandraVersion(major = 2.2)
    public void should_rethrow_read_failure_when_tombstone_overwhelm_on_replica() throws InterruptedException {
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            ccm = CCMBridge.create("test");

            // This relies on the fact that the previous line did not start the cluster, which is true
            // but a bit weird considering that other versions of create do
            // (JAVA-789 will fix this)
            ccm.updateConfig("tombstone_failure_threshold", "1000");
            ccm.populate(2);
            ccm.start();

            // The rest of the test relies on the fact that the PK '1' will be placed on node1
            // (hard-coding it is reasonable considering that C* shouldn't change its default partitioner too often)
            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(2))
                .withLoadBalancingPolicy(new WhiteListPolicy(Policies.defaultLoadBalancingPolicy(),
                    Lists.newArrayList(new InetSocketAddress(CCMBridge.ipOfNode(2), 9042))))
                .build();

            Session session = cluster.connect();
            session.execute("create KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("create table test.foo(pk int, cc int, v int, primary key (pk, cc))");

            // Generate 5000 tombstones on node1
            for (int i = 0; i < 5000; i++)
                session.execute("insert into test.foo (pk, cc, v) values (1, ?, null)", i);

            // Query to node2 (which will use node1 as replica, and therefore trigger the error)
            session.execute("select * from test.foo");
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }
}
