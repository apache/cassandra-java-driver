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

import org.mockito.ArgumentCaptor;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static com.datastax.driver.core.SchemaElement.KEYSPACE;
import static com.datastax.driver.core.SchemaElement.TABLE;
import static com.datastax.driver.core.TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.*;

@CreateCCM(PER_METHOD)
@CCMConfig(dirtiesContext = true, createKeyspace = false)
public class SchemaRefreshDebouncerTest extends CCMTestsSupport {

    // This may need to be tweaked depending on the reliability of the test environment.
    private static final int DEBOUNCE_TIME = 5000;

    // Control Connection to be spied.
    private ControlConnection controlConnection;

    // Schema Listener to be mocked.
    private SchemaChangeListener listener;

    // Separate session/clusters to observe schema events on.
    private Session session2;
    private Cluster cluster2;

    @BeforeMethod(groups = "short")
    public void setup() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setRefreshSchemaIntervalMillis(DEBOUNCE_TIME);
        queryOptions.setMaxPendingRefreshSchemaRequests(5);
        // Create a separate cluster that will receive the schema events on its control connection.
        cluster2 = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withQueryOptions(queryOptions)
                .build());
        session2 = cluster2.connect();

        // Create a spy of the Cluster's control connection and replace it with the spy.
        controlConnection = spy(cluster2.manager.controlConnection);
        cluster2.manager.controlConnection = controlConnection;

        // Create a mock of SchemaChangeListener to use for signalling events.
        listener = mock(SchemaChangeListener.class);
        cluster2.register(listener);

        reset(listener);
        reset(controlConnection);
    }

    /**
     * Ensures that when a CREATED and UPDATED schema_change events are received on a control
     * connection for the same keyspace within {@link QueryOptions#getRefreshSchemaIntervalMillis()}
     * that the schema refresh is debounced and coalesced into a single schema refresh for that keyspace only.
     *
     * @throws Exception
     * @jira_ticket JAVA-657
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_debounce_and_coalesce_create_and_alter_keyspace_into_refresh_keyspace() throws Exception {
        String keyspace = TestUtils.generateIdentifier("ks_");
        session().execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1));
        session().execute(String.format("ALTER KEYSPACE %s WITH DURABLE_WRITES=false", keyspace));

        ArgumentCaptor<KeyspaceMetadata> captor = forClass(KeyspaceMetadata.class);
        verify(listener, timeout(DEBOUNCE_TIME * 2).only()).onKeyspaceAdded(captor.capture());
        assertThat(captor.getValue()).hasName(keyspace).isNotDurableWrites();

        // Verify that the schema refresh was debounced and coalesced when a keyspace creation
        // and update event occur for the same keyspace.
        verify(controlConnection, times(1)).refreshSchema(KEYSPACE, keyspace, null, null);

        KeyspaceMetadata ksm = cluster2.getMetadata().getKeyspace(keyspace);
        // By ensuring durable writes is false, we know that the single schema refresh occurred
        // after the alter event.
        assertThat(ksm).isNotNull().hasName(keyspace).isNotDurableWrites();
    }


    /**
     * Ensures that when a CREATED (keyspace) and CREATED (table) schema_change events are received
     * on a control connection with that table belonging to that keyspace within
     * {@link QueryOptions#getRefreshSchemaIntervalMillis()} that the schema refresh is debounced
     * and coalesced into a single schema refresh for that keyspace only.
     *
     * @throws Exception
     * @jira_ticket JAVA-657
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_debounce_and_coalesce_create_keyspace_and_table_into_refresh_keyspace() throws Exception {
        String keyspace = TestUtils.generateIdentifier("ks_");
        String table = "tbl1";
        session().execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1));
        session().execute(String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", keyspace + "." + table));

        ArgumentCaptor<KeyspaceMetadata> keyspaceCaptor = forClass(KeyspaceMetadata.class);
        verify(listener, timeout(DEBOUNCE_TIME * 2).times(1)).onKeyspaceAdded(keyspaceCaptor.capture());
        assertThat(keyspaceCaptor.getValue()).hasName(keyspace);

        ArgumentCaptor<TableMetadata> tableCaptor = forClass(TableMetadata.class);
        verify(listener, timeout(DEBOUNCE_TIME * 2).times(1)).onTableAdded(tableCaptor.capture());
        assertThat(tableCaptor.getValue()).hasName(table);

        // Verify the schema refresh was debounced and coalesced when a keyspace event and table event
        // in that keyspace is detected.
        verify(controlConnection).refreshSchema(KEYSPACE, keyspace, null, null);
        verify(controlConnection, never()).refreshSchema(TABLE, keyspace, table, null);

        KeyspaceMetadata ksm = cluster2.getMetadata().getKeyspace(keyspace);
        assertThat(ksm).isNotNull();
        assertThat(ksm.getTable(table)).isNotNull();
    }

    /**
     * Ensures that when multiple CREATED schema_change events are received
     * on a control connection for tables belonging to the same keyspace within
     * {@link QueryOptions#getRefreshSchemaIntervalMillis()} that the schema refresh is debounced
     * and coalesced into a single schema refresh for that keyspace only.
     *
     * @throws Exception
     * @jira_ticket JAVA-657
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_debounce_and_coalesce_tables_in_same_keyspace_into_refresh_keyspace() throws Exception {
        String keyspace = TestUtils.generateIdentifier("ks_");
        session2.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1));
        // Reset invocations as creating keyspace causes a keyspace refresh.
        reset(controlConnection);
        reset(listener);

        int tableCount = 3;
        for (int i = 0; i < tableCount; i++) {
            session().execute(String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", keyspace + "." + "tbl" + i));
        }

        verify(listener, timeout(DEBOUNCE_TIME * 3).times(3)).onTableAdded(any(TableMetadata.class));

        // Verify a refresh of the keyspace was executed, but not individually on the
        // tables since those events were coalesced.
        verify(controlConnection).refreshSchema(KEYSPACE, keyspace, null, null);

        KeyspaceMetadata ksm = cluster2.getMetadata().getKeyspace(keyspace);
        assertThat(ksm).isNotNull();
        // metadata is present for each table.
        for (int i = 0; i < tableCount; i++) {
            String table = "tbl" + i;
            // Should have never been a refreshSchema on the table.
            verify(controlConnection, never()).refreshSchema(TABLE, keyspace, table, null);
            assertThat(ksm.getTable(table)).isNotNull();
        }
    }

    /**
     * Ensures that when multiple UPDATED schema_change events are received
     * on a control connection for for the same table within
     * {@link QueryOptions#getRefreshSchemaIntervalMillis()} that the schema refresh is debounced
     * and coalesced into a single schema refresh for that table only.
     *
     * @throws Exception
     * @jira_ticket JAVA-657
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_debounce_and_coalesce_multiple_alter_events_on_same_table_into_refresh_table() throws Exception {
        if (ccm().getCassandraVersion().compareTo(VersionNumber.parse("2.2")) >= 0)
            throw new SkipException("Disabled in Cassandra 2.2+ because of CASSANDRA-9996");

        String keyspace = TestUtils.generateIdentifier("ks_");
        String table = "tbl1";
        String comment = "I am changing this table.";
        String columnName = "added_column";
        // Execute on session 2 which refreshes schema as part of processing responses.
        session2.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1));
        session2.execute(String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", keyspace + "." + table));
        reset(controlConnection);
        reset(listener);

        session().execute(String.format("ALTER TABLE %s.%s WITH comment = '%s'", keyspace, table, comment));
        session().execute(String.format("ALTER TABLE %s.%s ADD %s int", keyspace, table, columnName));

        ArgumentCaptor<TableMetadata> original = forClass(TableMetadata.class);
        ArgumentCaptor<TableMetadata> captor = forClass(TableMetadata.class);
        verify(listener, timeout(DEBOUNCE_TIME * 2).times(1)).onTableChanged(captor.capture(), original.capture());
        assertThat(captor.getValue())
                .hasName(table)
                .isInKeyspace(keyspace)
                .hasColumn(columnName)
                .hasComment(comment);

        assertThat(original.getValue())
                .hasName(table)
                .isInKeyspace(keyspace)
                .hasNoColumn(columnName)
                .doesNotHaveComment(comment);

        // Verify a refresh of the table was executed, but only once.
        verify(controlConnection, times(1)).refreshSchema(TABLE, keyspace, table, Collections.<String>emptyList());

        KeyspaceMetadata ksm = cluster2.getMetadata().getKeyspace(keyspace);
        assertThat(ksm).isNotNull();
        TableMetadata tm = ksm.getTable(table);
        assertThat(tm)
                .hasName(table)
                .isInKeyspace(keyspace)
                .hasColumn(columnName)
                .hasComment(comment);
    }

    /**
     * Ensures that when a CREATED (keyspace) and CREATED (keyspace) schema_change events are received
     * on a control connection for different keyspaces within
     * {@link QueryOptions#getRefreshSchemaIntervalMillis()} that the schema refresh is debounced
     * and coalesced into a single full schema refresh.
     *
     * @throws Exception
     * @jira_ticket JAVA-657
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_debounce_and_coalesce_multiple_keyspace_creates_into_refresh_entire_schema() throws Exception {
        String prefix = TestUtils.generateIdentifier("ks_");
        for (int i = 0; i < 3; i++) {
            session().execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, prefix + i, 1));
            // check that the metadata is immediately up-to-date for the client that issued the DDL statement
            assertThat(cluster().getMetadata().getKeyspace(prefix + i)).isNotNull();
        }

        verify(listener, timeout(DEBOUNCE_TIME * 3).times(3)).onKeyspaceAdded(any(KeyspaceMetadata.class));
        // Verify a complete schema refresh was executed, but only once.
        verify(controlConnection, times(1)).refreshSchema(null, null, null, null);

        for (int i = 0; i < 3; i++) {
            KeyspaceMetadata ksm = cluster2.getMetadata().getKeyspace(prefix + i);
            assertThat(ksm).isNotNull().hasName(prefix + i);
        }
    }

    /**
     * Ensures that when enough schema changes have been received on a control connection to
     * reach {@link QueryOptions#getMaxPendingRefreshSchemaRequests()} that a schema refresh
     * is submitted right away.
     *
     * @throws Exception
     * @jira_ticket JAVA-657
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_refresh_when_max_pending_requests_reached() throws Exception {
        String prefix = TestUtils.generateIdentifier("ks_");
        for (int i = 0; i < 5; i++) {
            session().execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, prefix + i, 1));
            // check that the metadata is immediately up-to-date for the client that issued the DDL statement
            assertThat(cluster().getMetadata().getKeyspace(prefix + i)).isNotNull();
        }

        // Event should be processed immediately as we hit our threshold.
        verify(listener, timeout(DEBOUNCE_TIME * 5).times(5)).onKeyspaceAdded(any(KeyspaceMetadata.class));
        // Verify a complete schema refresh was executed, but only once.
        verify(controlConnection, times(1)).refreshSchema(null, null, null, null);

        for (int i = 0; i < 5; i++) {
            KeyspaceMetadata ksm = cluster2.getMetadata().getKeyspace(prefix + i);
            assertThat(ksm).isNotNull().hasName(prefix + i);
        }
    }
}
