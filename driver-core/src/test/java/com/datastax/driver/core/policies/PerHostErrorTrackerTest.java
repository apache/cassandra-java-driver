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
package com.datastax.driver.core.policies;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ScassandraCluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import org.scassandra.Scassandra;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.scassandra.http.client.PrimingRequest.queryBuilder;
import static org.scassandra.http.client.Result.success;
import static org.scassandra.http.client.Result.unauthorized;

public class PerHostErrorTrackerTest {

    @Test
    public void meterTest() throws InterruptedException {
        ScassandraCluster sCluster = ScassandraCluster.builder()
                .withNodes(3)
                .build();

        String query = "SELECT foo FROM bar";
        sCluster.init();

        for (Scassandra scassandra : sCluster.nodes()) {
            scassandra.primingClient().prime(
                    queryBuilder()
                            .withQuery(query)
                            .withResult(success)
                            .build()
            );
        }

        sCluster.node(1).primingClient().prime(queryBuilder()
                        .withQuery(query)
                        .withResult(unauthorized)
                        .build()
        );

        LoadBalancingPolicy lbp =
                ErrorAwarePolicy.builder(new AlwaysSameHostsPolicy())
                        .withInclusionThreshold(0.0000001)
                        .withRetryPeriod(100000)
                        .build();

        Cluster cluster = Cluster.builder()
                .addContactPoints(sCluster.address(1).getAddress(), sCluster.address(2).getAddress(), sCluster.address(3).getAddress())
                .withPort(sCluster.getBinaryPort())
                .withLoadBalancingPolicy(lbp)
                .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
                .build();

        Session session = cluster.connect();

        try {
            // will send to node1, which will respond with an error.
            session.execute(query);
        } catch (UnauthorizedException e) {
            assertThat(e.getAddress()).isEqualTo(sCluster.address(1));
        }

        // For some reason, it takes a certain time for the Meter to be updated when it's at 0...
        // So the previous is accounted, but it only appears to the tracker a little bit later.
        Thread.sleep(7000);

        try {
            // will send to node1, which will respond with an error.
            session.execute(query);
            fail("Should have failed after sending query to node1");
        } catch (UnauthorizedException e) {
            assertThat(e.getAddress()).isEqualTo(sCluster.address(1));
        }

        Thread.sleep(1000);

        ResultSet rs = session.execute(query);
        assertThat(rs.getExecutionInfo().getQueriedHost().getSocketAddress()).isEqualTo(sCluster.address(2));
    }

    @Test
    public void meterTest2() throws InterruptedException {
        ScassandraCluster sCluster = ScassandraCluster.builder()
                .withNodes(3)
                .build();

        String query = "SELECT foo FROM bar";
        sCluster.init();

        for (Scassandra scassandra : sCluster.nodes()) {
            scassandra.primingClient().prime(
                    queryBuilder()
                            .withQuery(query)
                            .withResult(success)
                            .build()
            );
        }

        sCluster.node(1).primingClient().prime(queryBuilder()
                        .withQuery(query)
                        .withResult(unauthorized)
                        .build()
        );

        LoadBalancingPolicy lbp =
                ErrorAwarePolicy.builder(new AlwaysSameHostsPolicy())
                        .withInclusionThreshold(0.2)
                        .withRetryPeriod(1000)
                        .build();

        Cluster cluster = Cluster.builder()
                .addContactPoints(sCluster.address(1).getAddress(), sCluster.address(2).getAddress(), sCluster.address(3).getAddress())
                .withPort(sCluster.getBinaryPort())
                .withLoadBalancingPolicy(lbp)
                .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
                .build();

        Session session = cluster.connect();

        try {
            // will send to node1, which will respond with an error.
            session.execute(query);
            fail("Should have failed after sending query to node1");
        } catch (UnauthorizedException e) {
            assertThat(e.getAddress()).isEqualTo(sCluster.address(1));
        }

        Thread.sleep(7000);

        try {
            // will send to node1, which will respond with an error.
            session.execute(query);
            fail("Should have failed after sending query to node1");
        } catch (UnauthorizedException e) {
            assertThat(e.getAddress()).isEqualTo(sCluster.address(1));
        }
        // now node1 excluded

        // wait until it is reconsidered
        Thread.sleep(2000);

        try {
            // will send to node1, which will respond with an error.
            session.execute(query);
            fail("Should have failed after sending query to node1");
        } catch (UnauthorizedException e) {
            assertThat(e.getAddress()).isEqualTo(sCluster.address(1));
        }

    }



    private class AlwaysSameHostsPolicy implements LoadBalancingPolicy {

        Collection<Host> hosts;

        @Override
        public void init(Cluster cluster, Collection<Host> hosts) {
            // hosts may have been shuffled so need to sort in ascending order
            List<Host> copyHosts = new ArrayList<Host>(hosts);
            Collections.sort(copyHosts, new Comparator<Host>() {
                @Override
                public int compare(Host o1, Host o2) {
                    byte[] ba1 = o1.getAddress().getAddress();
                    byte[] ba2 = o2.getAddress().getAddress();

                    // general ordering: ipv4 before ipv6
                    if (ba1.length < ba2.length) return -1;
                    if (ba1.length > ba2.length) return 1;

                    // we have 2 ips of the same type, so we have to compare each byte
                    for (int i = 0; i < ba1.length; i++) {
                        int b1 = unsignedByteToInt(ba1[i]);
                        int b2 = unsignedByteToInt(ba2[i]);
                        if (b1 == b2)
                            continue;
                        if (b1 < b2)
                            return -1;
                        else
                            return 1;
                    }
                    return 0;
                }

                private int unsignedByteToInt(byte b) {
                    return (int) b & 0xFF;
                }
            });
            this.hosts = copyHosts;
        }

        @Override
        public HostDistance distance(Host host) {
            return HostDistance.LOCAL;
        }

        @Override
        public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            return hosts.iterator();
        }

        @Override
        public void onAdd(Host host) {

        }

        @Override
        public void onUp(Host host) {

        }

        @Override
        public void onDown(Host host) {

        }

        @Override
        public void onRemove(Host host) {

        }

        @Override
        public void close() {

        }
    }
}
