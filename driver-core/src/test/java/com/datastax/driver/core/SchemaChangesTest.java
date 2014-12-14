package com.datastax.driver.core;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.utils.Bytes;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class SchemaChangesTest extends CCMBridge.PerClassSingleNodeCluster {

    // Create a second cluster to check that other clients also get notified
    Cluster cluster2;
    // The metadatas of the two clusters should be kept in sync
    List<Metadata> metadatas;

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList();
    }

    @BeforeClass(groups = "short")
    public void setup() {
        cluster2 = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1)).build();

        cluster.manager.controlConnection = spy(cluster.manager.controlConnection);
        cluster2.manager.controlConnection = spy(cluster2.manager.controlConnection);
        cluster.manager.metadata = spy(cluster.manager.metadata);
        cluster2.manager.metadata = spy(cluster2.manager.metadata);

        metadatas = Lists.newArrayList(cluster.getMetadata(), cluster2.getMetadata());
    }

    /**
     * Test that schema refresh are as specific as possible for a CREATE TABLE event.
     *
     * This method verifies that when a table is created in a keyspace, the Cluster manager only fetches metadata for
     * that table, and not for the whole keyspace.
     *
     * First, we wait for the Cluster instance to be initialized.  We then spy what happens on its controlConnection.
     * Once the CREATE TABLE statement has been issued, we verify that the method ControlConnection#refreshSchema has
     * been called with "ks" and "created_table" as parameter, thus targeting this table only.
     *
     * Note: a possible improvement for that test would be to verify that no call to controlConnection.refreshSchema is
     * made with other parameters that "ks" and "created_table".
     *
     * @since 2.0
     * @jira_ticket JAVA-543
     * @expected_result The ControlConnection.refreshSchema method is called with the keyspace and table created
     */
    @Test(groups = "short")
    public void should_notify_of_table_creation() throws InterruptedException {
        resetSpies();
        session.execute("CREATE TABLE ks.created_table(i int primary key)");

        for (Metadata m : metadatas)
            assertThat(m.getKeyspace("ks").getTable("created_table")).isNotNull();
        verify(cluster.manager.controlConnection).refreshSchema("ks", "created_table");
        verify(cluster2.manager.controlConnection).refreshSchema("ks", "created_table");
    }

    /**
     * Test that schema refresh are as specific as possible for a ALTER TABLE event.
     *
     * This method verifies that when a table is altered in a keyspace, the Cluster manager only fetches metadata for
     * that table, and not for the whole keyspace.
     *
     * First, we wait for the Cluster instance to be initialized and for the CREATE TABLE statement to be issued. We
     * then spy what happens on its controlConnection. Once the ALTER TABLE statement has been issued, we verify that
     * the method ControlConnection#refreshSchema has been called with "ks" and "updated_table" as parameter, thus
     * targeting this table only.
     *
     * Note: a possible improvement for that test would be to verify that no call to controlConnection.refreshSchema is
     * made with other parameters that "ks" and "updated_table".
     *
     * @since 2.0
     * @jira_issue JAVA-543
     * @expected_result The ControlConnection.refreshSchema method is called with the keyspace and table altered
     */
    @Test(groups = "short")
    public void should_notify_of_table_update() throws InterruptedException {
        session.execute("CREATE TABLE ks.updated_table(i int primary key)");

        resetSpies();
        session.execute("ALTER TABLE ks.updated_table ADD j int");

        for (Metadata m : metadatas)
            assertThat(m.getKeyspace("ks").getTable("updated_table").getColumn("j")).isNotNull();
        verify(cluster.manager.controlConnection).refreshSchema("ks", "updated_table");
        verify(cluster2.manager.controlConnection).refreshSchema("ks", "updated_table");
    }

    /**
     * Test that schema refresh are as specific as possible for a DROP TABLE event.
     *
     * This method verifies that when a table is removed from a keyspace, the Cluster manager does not fetch any
     * metadata but rather use the change event to update its internal keyspace representation.
     *
     * First, we wait for the Cluster instance to be initialized and for the CREATE TABLE statement to be issued. We
     * then spy what happens on its controlConnection. Once the DROP TABLE statement has been issued, we verify that
     * the method Metadata#removeTable has been called with "ks" and "deleted_table" as parameter, thus
     * targeting this table only.
     *
     * @since 2.0
     * @jira_issue JAVA-543
     * @expected_result The ControlConnection.refreshSchema method is called with the keyspace and table altered
     */
    @Test(groups = "short")
    public void should_notify_of_table_drop() throws InterruptedException {
        session.execute("CREATE TABLE ks.deleted_table(i int primary key)");

        resetSpies();
        session.execute("DROP TABLE ks.deleted_table");

        for (Metadata m : metadatas)
            assertThat(m.getKeyspace("ks").getTable("deleted_table")).isNull();
        // Note that atLeastOnce() required here. We try to remove the table twice.
        verify(cluster.getMetadata(), atLeastOnce()).removeTable("ks", "deleted_table");
        verify(cluster2.getMetadata()).removeTable("ks", "deleted_table");
    }

    /**
     * Test that schema refresh are as specific as possible for a CREATE KEYSPACE event.
     *
     * This method verifies that when a keyspace is created, the Cluster manager only fetches metadata for that
     * keyspace.
     *
     * First, we wait for the Cluster instance to be initialized.  We then spy what happens on its controlConnection.
     * Once the CREATE KEYSPACE statement has been issued, we verify that the method ControlConnection#refreshSchema has
     * been called with "ks2" as parameter, thus targeting this keyspace only.
     *
     * Note: a possible improvement for that test would be to verify that no call to controlConnection.refreshSchema is
     * made with other parameters that "ks2".
     *
     * @since 2.0
     * @jira_ticket JAVA-543
     * @expected_result The ControlConnection.refreshSchema method is called with the keyspace and table created
     */
    @Test(groups = "short")
    public void should_notify_of_keyspace_creation() throws InterruptedException {
        resetSpies();
        session.execute("CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

        for (Metadata m : metadatas)
            assertThat(m.getKeyspace("ks2")).isNotNull();
        verify(cluster.manager.controlConnection).refreshSchema("ks2", null);
        verify(cluster2.manager.controlConnection).refreshSchema("ks2", null);
    }

    /**
     * Test that schema refresh are as specific as possible for a ALTER KEYSPACE event.
     *
     * This method verifies that when a keyspace is altered, the Cluster manager only fetches metadata for that
     * keyspace.
     *
     * First, we wait for the Cluster instance to be initialized.  We then spy what happens on its controlConnection.
     * Once the ALTER KEYSPACE statement has been issued, we verify that the method ControlConnection#refreshSchema has
     * been called with "ks2" as parameter, thus targeting this keyspace only.
     *
     * @jira_ticket JAVA-543
     * @expected_result The ControlConnection.refreshSchema method is called with the keyspace and table created
     * @since 2.0
     */
    @Test(groups = "short")
    public void should_notify_of_keyspace_update() throws InterruptedException {
        session.execute("CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace("ks2").isDurableWrites()).isTrue();

        resetSpies();
        session.execute("ALTER KEYSPACE ks2 WITH durable_writes = false");

        for (Metadata m : metadatas)
            assertThat(m.getKeyspace("ks2").isDurableWrites()).isFalse();
        verify(cluster.manager.controlConnection).refreshSchema("ks2", null);
        verify(cluster2.manager.controlConnection).refreshSchema("ks2", null);
    }

    /**
     * Test that schema refresh are as specific as possible for a DROP KEYSPACE event.
     *
     * This method verifies that when a keyspace is dropped, the Cluster manager does not fetch any metadata but rather
     * use the change event to update its internal database representation.
     *
     * First, we wait for the Cluster instance to be initialized and for the CREATE KEYSPACE statement to be issued. We
     * then spy what happens on its controlConnection. Once the DROP KEYSPACE statement has been issued, we verify that
     * the method Metadata#removeKeyspace has been called with "ks2" as parameter, thus targeting this table only.
     *
     * @jira_issue JAVA-543
     * @expected_result The ControlConnection.refreshSchema method is called with the keyspace and table altered
     * @since 2.0
     */
    @Test(groups = "short")
    public void should_notify_of_keyspace_drop() {
        session.execute("CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        for (Metadata m : metadatas)
            assertThat(m.getReplicas("ks2", Bytes.fromHexString("0xCAFEBABE"))).isNotEmpty();

        resetSpies();
        session.execute("DROP KEYSPACE ks2");

        for (Metadata m : metadatas) {
            assertThat(m.getKeyspace("ks2")).isNull();
            assertThat(m.getReplicas("ks2", Bytes.fromHexString("0xCAFEBABE"))).isEmpty();
        }
        // Note that atLeastOnce() required here. We try to remove the keyspace twice.
        verify(cluster.getMetadata(), atLeastOnce()).removeKeyspace("ks2");
        verify(cluster2.getMetadata()).removeKeyspace("ks2");
    }

    /**
     * Resets the spies counters for clusters control connections and metadata.
     *
     * This method is necessary since the {@code Cluster} instances are shared between tests.
     */
    private void resetSpies() {
        Mockito.reset(cluster.manager.controlConnection);
        Mockito.reset(cluster2.manager.controlConnection);
        Mockito.reset(cluster.manager.metadata);
        Mockito.reset(cluster2.manager.metadata);
    }

    @AfterMethod(groups = "short")
    public void cleanup() {
        session.execute("DROP TABLE IF EXISTS ks.table1");
        session.execute("DROP KEYSPACE IF EXISTS ks2");
    }

    @AfterClass(groups = "short")
    public void teardown() {
        if (cluster2 != null)
            cluster2.close();
    }
}
