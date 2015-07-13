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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.datastax.driver.core.utils.Bytes;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.Metadata.handleId;

public class SchemaChangesTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }";
    private static final String ALTER_KEYSPACE = "ALTER KEYSPACE %s WITH durable_writes = false";
    private static final String DROP_KEYSPACE = "DROP KEYSPACE %s";

    private static final String CREATE_TABLE = "CREATE TABLE %s.table1(i int primary key)";
    private static final String ALTER_TABLE = "ALTER TABLE %s.table1 ADD j int";
    private static final String DROP_TABLE = "DROP TABLE %s.table1";

    /** The maximum time that the test will wait to check that listeners have been notified */
    private static final int NOTIF_TIMEOUT_MS = 5000;

    Cluster cluster1;
    Cluster cluster2; // a second cluster to check that other clients also get notified

    // The metadatas of the two clusters (we'll test that they're kept in sync)
    List<Metadata> metadatas;

    Session session1;

    SchemaChangeListener listener1;
    SchemaChangeListener listener2;

    List<SchemaChangeListener> listeners;

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList();
    }

    @BeforeClass(groups = "short")
    public void setup() throws InterruptedException {
        Cluster.Builder builder = configure(Cluster.builder())
            .addContactPointsWithPorts(Collections.singletonList(hostAddress))
            .withQueryOptions(new QueryOptions()
                    .setRefreshNodeIntervalMillis(0)
                    .setRefreshNodeListIntervalMillis(0)
                    .setRefreshSchemaIntervalMillis(0)
            );
        cluster1 = builder.build();
        cluster2 = builder.build();

        metadatas = Lists.newArrayList(cluster1.getMetadata(), cluster2.getMetadata());

        session1 = cluster1.connect();

        cluster1.register(listener1 = mock(SchemaChangeListener.class));
        cluster1.register(listener2 = mock(SchemaChangeListener.class));
        listeners = Lists.newArrayList(listener1, listener2);

        execute(CREATE_KEYSPACE, "lowercase");
        execute(CREATE_KEYSPACE, "\"CaseSensitive\"");
    }

    @AfterClass(groups = "short")
    public void teardown() {
        if (cluster1 != null)
            cluster1.close();
        if (cluster2 != null)
            cluster2.close();
    }

    @DataProvider(name = "existingKeyspaceName")
    public static Object[][] existingKeyspaceName() {
        return new Object[][]{ { "lowercase" }, { "\"CaseSensitive\"" } };
    }

    @DataProvider(name = "newKeyspaceName")
    public static Object[][] newKeyspaceName() {
        return new Object[][]{ { "lowercase2" }, { "\"CaseSensitive2\"" } };
    }

    @BeforeMethod(groups = "short")
    public void resetListeners() {
        for (SchemaChangeListener listener : listeners) {
            reset(listener);
        }
    }

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    public void should_notify_of_table_creation(String keyspace) throws InterruptedException {
        execute(CREATE_TABLE, keyspace);
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<TableMetadata> added = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableAdded(added.capture());
            assertThat(added.getValue())
                .isInKeyspace(handleId(keyspace))
                .hasName("table1");
        }
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).getTable("table1")).isNotNull();
    }

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    public void should_notify_of_table_update(String keyspace) throws InterruptedException {
        execute(CREATE_TABLE, keyspace);
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).getTable("table1")).hasNoColumn("j");
        ArgumentCaptor<TableMetadata> added = null;
        for (SchemaChangeListener listener : listeners) {
            added = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableAdded(added.capture());
            assertThat(added.getValue())
                .isInKeyspace(handleId(keyspace))
                .hasName("table1");
        }
        execute(ALTER_TABLE, keyspace);
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<TableMetadata> current = ArgumentCaptor.forClass(TableMetadata.class);
            ArgumentCaptor<TableMetadata> previous = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableChanged(current.capture(), previous.capture());
            assertThat(previous.getValue())
                .isEqualTo(added.getValue())
                .hasNoColumn("j");
            assertThat(current.getValue())
                .isInKeyspace(handleId(keyspace))
                .hasName("table1")
                .hasColumn("j");
        }
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).getTable("table1")).hasColumn("j");
    }

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    public void should_notify_of_table_drop(String keyspace) throws InterruptedException {
        execute(CREATE_TABLE, keyspace);
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<TableMetadata> added = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableAdded(added.capture());
            assertThat(added.getValue())
                .hasName("table1")
                .isInKeyspace(handleId(keyspace));
        }
        execute(DROP_TABLE, keyspace);
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).getTable("table1")).isNull();
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<TableMetadata> removed = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableRemoved(removed.capture());
            assertThat(removed.getValue()).hasName("table1");
        }
    }

    @Test(groups = "short", dataProvider = "newKeyspaceName")
    public void should_notify_of_keyspace_creation(String keyspace) throws InterruptedException {
        execute(CREATE_KEYSPACE, keyspace);
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace)).isNotNull();
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<KeyspaceMetadata> added = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onKeyspaceAdded(added.capture());
            assertThat(added.getValue()).hasName(handleId(keyspace));
        }
    }

    @Test(groups = "short", dataProvider = "newKeyspaceName")
    public void should_notify_of_keyspace_update(String keyspace) throws InterruptedException {
        execute(CREATE_KEYSPACE, keyspace);
        ArgumentCaptor<KeyspaceMetadata> added = null;
        for (SchemaChangeListener listener : listeners) {
            added = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onKeyspaceAdded(added.capture());
            assertThat(added.getValue()).hasName(handleId(keyspace));
        }
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).isDurableWrites()).isTrue();
        execute(ALTER_KEYSPACE, keyspace);
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<KeyspaceMetadata> current = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            ArgumentCaptor<KeyspaceMetadata> previous = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onKeyspaceChanged(current.capture(), previous.capture());
            assertThat(previous.getValue())
                .isEqualTo(added.getValue())
                .isDurableWrites();
            assertThat(current.getValue())
                .hasName(handleId(keyspace))
                .isNotDurableWrites();
        }
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace)).isNotDurableWrites();
    }

    @Test(groups = "short", dataProvider = "newKeyspaceName")
    public void should_notify_of_keyspace_drop(String keyspace) throws InterruptedException {
        execute(CREATE_KEYSPACE, keyspace);
        for (Metadata m : metadatas)
            assertThat(m.getReplicas(keyspace, Bytes.fromHexString("0xCAFEBABE"))).isNotEmpty();
        execute(CREATE_TABLE, keyspace); // to test table drop notifications
        execute(DROP_KEYSPACE, keyspace);
        for (SchemaChangeListener listener : listeners) {
            ArgumentCaptor<TableMetadata> table = ArgumentCaptor.forClass(TableMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableRemoved(table.capture());
            assertThat(table.getValue())
                .hasName("table1")
                .isInKeyspace(handleId(keyspace));

            ArgumentCaptor<KeyspaceMetadata> ks = ArgumentCaptor.forClass(KeyspaceMetadata.class);
            verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onKeyspaceRemoved(ks.capture());
            assertThat(ks.getValue())
                .hasName(handleId(keyspace));
        }
        for (Metadata m : metadatas) {
            assertThat(m.getKeyspace(keyspace)).isNull();
            assertThat(m.getReplicas(keyspace, Bytes.fromHexString("0xCAFEBABE"))).isEmpty();
        }
    }

    @AfterMethod(groups = "short")
    public void cleanup() throws InterruptedException {
        ListenableFuture<List<ResultSet>> f = Futures.successfulAsList(Lists.newArrayList(
            session1.executeAsync("DROP TABLE lowercase.table1"),
            session1.executeAsync("DROP TABLE \"CaseSensitive\".table1"),
            session1.executeAsync("DROP KEYSPACE lowercase2"),
            session1.executeAsync("DROP KEYSPACE \"CaseSensitive2\"")
        ));
        Futures.getUnchecked(f);
    }

    private void execute(String cql, String keyspace) throws InterruptedException {
        session1.execute(String.format(cql, keyspace));
    }
}
