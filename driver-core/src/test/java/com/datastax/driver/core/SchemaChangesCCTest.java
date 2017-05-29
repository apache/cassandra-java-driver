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

import com.datastax.driver.core.policies.DelegatingLoadBalancingPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.google.common.util.concurrent.Uninterruptibles;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@CCMConfig(numberOfNodes = 2, dirtiesContext = true, createCluster = false)
public class SchemaChangesCCTest extends CCMTestsSupport {

    private static final int NOTIF_TIMEOUT_MS = 5000;

    /**
     * Validates that any schema change events made while the control connection is down are
     * propagated when the control connection is re-established.
     * <p/>
     * <p/>
     * Note that on control connection recovery not all schema changes are propagated.  For example,
     * if a table was altered and then dropped only a drop event would be received as that is all
     * that can be discerned.
     *
     * @test_category control_connection, schema
     * @expected_result keyspace and table add, drop, and remove events are propagated on control connection reconnect.
     * @jira_ticket JAVA-151
     * @since 2.0.11, 2.1.8, 2.2.1
     */
    @Test(groups = "long")
    public void should_receive_changes_made_while_control_connection_is_down_on_reconnect() throws Exception {
        ToggleablePolicy lbPolicy = new ToggleablePolicy(Policies.defaultLoadBalancingPolicy());
        Cluster cluster = register(
                Cluster.builder()
                        .withLoadBalancingPolicy(lbPolicy)
                        .addContactPoints(getContactPoints().get(0))
                        .withPort(ccm().getBinaryPort())
                        .build());
        // Put cluster2 control connection on node 2 so it doesn't go down (to prevent noise for debugging).
        Cluster cluster2 = register(
                Cluster.builder()
                        .withLoadBalancingPolicy(lbPolicy)
                        .addContactPoints(getContactPoints().get(1))
                        .withPort(ccm().getBinaryPort())
                        .build());
        SchemaChangeListener listener = mock(SchemaChangeListener.class);

        cluster.init();
        cluster.register(listener);

        Session session2 = cluster2.connect();
        // Create two keyspaces to experiment with.
        session2.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "ks1", 1));
        session2.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "ks2", 1));
        session2.execute("create table ks1.tbl1 (k text primary key, v text)");
        session2.execute("create table ks1.tbl2 (k text primary key, v text)");

        // Wait for both create events to be received.
        verify(listener, timeout(NOTIF_TIMEOUT_MS).times(2)).onKeyspaceAdded(any(KeyspaceMetadata.class));
        verify(listener, timeout(NOTIF_TIMEOUT_MS).times(2)).onTableAdded(any(TableMetadata.class));


        KeyspaceMetadata prealteredKeyspace = cluster.getMetadata().getKeyspace("ks1");
        KeyspaceMetadata predroppedKeyspace = cluster.getMetadata().getKeyspace("ks2");
        TableMetadata prealteredTable = cluster.getMetadata().getKeyspace("ks1").getTable("tbl1");
        TableMetadata predroppedTable = cluster.getMetadata().getKeyspace("ks1").getTable("tbl2");

        // Enable returning empty query plan for default statements.  This will
        // prevent the control connection from being able to reconnect.
        lbPolicy.returnEmptyQueryPlan = true;

        // Stop node 1, which hosts the control connection.
        ccm().stop(1);
        assertThat(cluster).host(1).goesDownWithin(20, TimeUnit.SECONDS);

        // Ensure control connection is down.
        assertThat(cluster.manager.controlConnection.isOpen()).isFalse();

        // Perform some schema changes that we'll validate when the control connection comes back.
        session2.execute("drop keyspace ks2");
        session2.execute("drop table ks1.tbl2");
        session2.execute("alter keyspace ks1 with durable_writes=false");
        session2.execute("alter table ks1.tbl1 add new_col varchar");
        session2.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "ks3", 1));
        session2.execute("create table ks1.tbl3 (k text primary key, v text)");

        // Reset the mock to clear invocations. (sanity check to ensure all events happen after CC comes back up)
        reset(listener);

        // Switch the flag so the control connection may now be established.
        lbPolicy.returnEmptyQueryPlan = false;

        // Poll on the control connection and wait for it to be reestablished.
        long maxControlConnectionWait = 60000;
        long startTime = System.currentTimeMillis();
        while (!cluster.manager.controlConnection.isOpen() && System.currentTimeMillis() - startTime < maxControlConnectionWait) {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }

        assertThat(cluster.manager.controlConnection.isOpen())
                .as("Control connection was not opened after %dms.", maxControlConnectionWait)
                .isTrue();

        // Ensure the drop keyspace event shows up.
        ArgumentCaptor<KeyspaceMetadata> removedKeyspace = ArgumentCaptor.forClass(KeyspaceMetadata.class);
        verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onKeyspaceRemoved(removedKeyspace.capture());
        assertThat(removedKeyspace.getValue())
                .hasName("ks2")
                .isEqualTo(predroppedKeyspace);

        // Ensure the drop table event shows up.
        ArgumentCaptor<TableMetadata> droppedTable = ArgumentCaptor.forClass(TableMetadata.class);
        verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableRemoved(droppedTable.capture());

        assertThat(droppedTable.getValue())
                .isInKeyspace("ks1")
                .hasName("tbl2")
                .isEqualTo(predroppedTable);

        // Ensure that the alter keyspace event shows up.
        ArgumentCaptor<KeyspaceMetadata> alteredKeyspace = ArgumentCaptor.forClass(KeyspaceMetadata.class);
        ArgumentCaptor<KeyspaceMetadata> originalKeyspace = ArgumentCaptor.forClass(KeyspaceMetadata.class);
        verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onKeyspaceChanged(alteredKeyspace.capture(), originalKeyspace.capture());

        // Previous metadata should match the metadata observed before disconnect.
        assertThat(originalKeyspace.getValue())
                .hasName("ks1")
                .isDurableWrites()
                .isEqualTo(prealteredKeyspace);

        // New metadata should reflect that the durable writes attribute changed.
        assertThat(alteredKeyspace.getValue())
                .hasName("ks1")
                .isNotDurableWrites();

        // Ensure the alter table event shows up.
        ArgumentCaptor<TableMetadata> alteredTable = ArgumentCaptor.forClass(TableMetadata.class);
        ArgumentCaptor<TableMetadata> originalTable = ArgumentCaptor.forClass(TableMetadata.class);
        verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableChanged(alteredTable.capture(), originalTable.capture());

        // Previous metadata should match the metadata observed before disconnect.
        assertThat(originalTable.getValue())
                .isInKeyspace("ks1")
                .hasName("tbl1")
                .doesNotHaveColumn("new_col")
                .isEqualTo(prealteredTable);

        // New metadata should reflect that the column type changed.
        assertThat(alteredTable.getValue())
                .isInKeyspace("ks1")
                .hasName("tbl1")
                .hasColumn("new_col", DataType.varchar());

        // Ensure the add keyspace event shows up.
        ArgumentCaptor<KeyspaceMetadata> addedKeyspace = ArgumentCaptor.forClass(KeyspaceMetadata.class);
        verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onKeyspaceAdded(addedKeyspace.capture());

        assertThat(addedKeyspace.getValue()).hasName("ks3");

        // Ensure the add table event shows up.
        ArgumentCaptor<TableMetadata> addedTable = ArgumentCaptor.forClass(TableMetadata.class);
        verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableAdded(addedTable.capture());

        assertThat(addedTable.getValue())
                .isInKeyspace("ks1")
                .hasName("tbl3");
    }

    /**
     * A load balancing policy that can be "disabled" by having its query plan return no hosts when
     * given the 'DEFAULT' statement.  This statement is used for retrieving query plan
     * and for finding what hosts to use for control connection.
     */
    public static class ToggleablePolicy extends DelegatingLoadBalancingPolicy {

        volatile boolean returnEmptyQueryPlan;

        public ToggleablePolicy(LoadBalancingPolicy delegate) {
            super(delegate);
        }

        @Override
        public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            if (returnEmptyQueryPlan && statement == Statement.DEFAULT)
                return Collections.<Host>emptyList().iterator();
            else
                return super.newQueryPlan(loggedKeyspace, statement);
        }
    }
}
