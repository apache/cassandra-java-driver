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

import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.google.common.util.concurrent.Uninterruptibles;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.TestUtils.nonDebouncingQueryOptions;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.scassandra.http.client.ClosedConnectionReport.CloseType.CLOSE;

public class HostConnectionPoolMultiTest {

    private ScassandraCluster scassandra;

    private Cluster cluster;

    @BeforeMethod(groups = {"short", "long"})
    private void setUp() {
        scassandra = ScassandraCluster.builder().withNodes(2).build();
        scassandra.init();
    }

    @AfterMethod(groups = {"short", "long"}, alwaysRun = true)
    private void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
        scassandra.stop();
    }

    private void createCluster(int core, int max) {
        PoolingOptions poolingOptions = new PoolingOptions().setConnectionsPerHost(LOCAL, core, max);
        SocketOptions socketOptions = new SocketOptions().setReadTimeoutMillis(1000);
        cluster = Cluster.builder()
                .addContactPoints(scassandra.address(1).getAddress())
                .withPort(scassandra.getBinaryPort())
                .withQueryOptions(nonDebouncingQueryOptions())
                .withPoolingOptions(poolingOptions)
                .withSocketOptions(socketOptions)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(1000))
                .build();
        cluster.connect();
    }

    /**
     * Ensures that if all connections fail to a host on pool init that the host is marked down.
     *
     * @jira_ticket JAVA-544
     * @test_category connection:connection_pool
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_mark_host_down_if_all_connections_fail_on_init() {
        // Prevent any connections on node 2.
        scassandra.node(2).currentClient().disableListener();
        createCluster(8, 8);

        // Node 2 should be in a down state while node 1 stays up.
        assertThat(cluster).host(2).goesDownWithin(10, SECONDS);
        assertThat(cluster).host(1).isUp();

        // Node 2 should come up as soon as it is able to reconnect.
        scassandra.node(2).currentClient().enableListener();
        assertThat(cluster).host(2).comesUpWithin(2, SECONDS);
    }

    /**
     * Ensures that if the control connection goes down, but the Host bound the control connection
     * still has an up pool, the Host should remain up and the Control Connection should be replaced.
     *
     * @jira_ticket JAVA-544
     * @test_category connection:connection_pool
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_replace_control_connection_if_it_goes_down_but_host_remains_up() {
        createCluster(1, 2);

        // Ensure control connection is on node 1.
        assertThat(cluster).usesControlHost(1);

        // Identify the socket associated with the control connection.
        Connection controlConnection = cluster.manager.controlConnection.connectionRef.get();
        InetSocketAddress controlSocket = (InetSocketAddress) controlConnection.channel.localAddress();

        // Close the control connection.
        scassandra.node(1).currentClient()
                .closeConnection(CLOSE, controlSocket);

        // Sleep reconnect interval * 2 to allow time to reconnect.
        Uninterruptibles.sleepUninterruptibly(2, SECONDS);

        // Ensure the control connection was replaced and host 1 remains up.
        assertThat(cluster).hasOpenControlConnection()
                .host(1).isUp();
        assertThat(cluster.manager.controlConnection.connectionRef.get()).isNotEqualTo(controlConnection);
    }
}
