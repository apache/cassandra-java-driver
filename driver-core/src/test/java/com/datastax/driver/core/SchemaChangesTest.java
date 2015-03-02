package com.datastax.driver.core;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.*;

import com.datastax.driver.core.utils.Bytes;

import static com.datastax.driver.core.Assertions.assertThat;

public class SchemaChangesTest extends CCMBridge.PerClassSingleNodeCluster {

    // Create a second cluster to check that other clients also get notified
    Cluster cluster2;
    // The metadatas of the two clusters should be kept in sync
    List<Metadata> metadatas;

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList("CREATE KEYSPACE \"CaseSensitive\" WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1' }");
    }

    @DataProvider(name = "existingKeyspaceName")
    public static Object[][] existingKeyspaceName() {
        return new Object[][]{ { "ks" }, { "\"CaseSensitive\"" } };
    }

    @BeforeClass(groups = "short")
    public void setup() {
        cluster2 = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1)).build();
        metadatas = Lists.newArrayList(cluster.getMetadata(), cluster2.getMetadata());
    }

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    public void should_notify_of_table_creation(String keyspace) {
        session.execute(String.format("CREATE TABLE %s.table1(i int primary key)", keyspace));

        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).getTable("table1"))
                .isNotNull();
    }

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    public void should_notify_of_table_update(String keyspace) {
        session.execute(String.format("CREATE TABLE %s.table1(i int primary key)", keyspace));
        session.execute(String.format("ALTER TABLE %s.table1 ADD j int", keyspace));

        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).getTable("table1").getColumn("j"))
                .isNotNull();
    }

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    public void should_notify_of_table_drop(String keyspace) {
        session.execute(String.format("CREATE TABLE %s.table1(i int primary key)", keyspace));
        session.execute(String.format("DROP TABLE %s.table1", keyspace));

        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).getTable("table1"))
                .isNull();
    }

    @DataProvider(name = "newKeyspaceName")
    public static Object[][] newKeyspaceName() {
        return new Object[][]{ { "ks2" }, { "\"CaseSensitive2\"" } };
    }

    @Test(groups = "short", dataProvider = "newKeyspaceName")
    public void should_notify_of_keyspace_creation(String keyspace) {
        session.execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace));

        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace))
                .isNotNull();
    }

    @Test(groups = "short", dataProvider = "newKeyspaceName")
    public void should_notify_of_keyspace_update(String keyspace) {
        session.execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace));
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).isDurableWrites())
                .isTrue();

        session.execute(String.format("ALTER KEYSPACE %s WITH durable_writes = false", keyspace));
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).isDurableWrites())
                .isFalse();
    }

    @Test(groups = "short", dataProvider = "newKeyspaceName")
    public void should_notify_of_keyspace_drop(String keyspace) {
        session.execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace));
        for (Metadata m : metadatas)
            assertThat(m.getReplicas(keyspace, Bytes.fromHexString("0xCAFEBABE")))
                .isNotEmpty();

        session.execute(String.format("DROP KEYSPACE %s", keyspace));

        for (Metadata m : metadatas) {
            assertThat(m.getKeyspace(keyspace))
                .isNull();
            assertThat(m.getReplicas(keyspace, Bytes.fromHexString("0xCAFEBABE")))
                .isEmpty();
        }
    }

    @AfterMethod(groups = "short")
    public void cleanup() {
        ListenableFuture<List<ResultSet>> f = Futures.successfulAsList(Lists.newArrayList(
            session.executeAsync("DROP TABLE ks.table1"),
            session.executeAsync("DROP TABLE \"CaseSensitive\".table1"),
            session.executeAsync("DROP KEYSPACE ks2"),
            session.executeAsync("DROP KEYSPACE \"CaseSensitive2\"")
        ));
        Futures.getUnchecked(f);
    }

    @AfterClass(groups = "short")
    public void teardown() {
        if (cluster2 != null)
            cluster2.close();
    }
}
