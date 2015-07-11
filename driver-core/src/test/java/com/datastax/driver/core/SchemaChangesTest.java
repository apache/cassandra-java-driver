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

import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.*;

import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.CassandraVersion;

import static com.datastax.driver.core.Assertions.assertThat;

public class SchemaChangesTest {
    private static final String CREATE_KEYSPACE =
        "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }";

    CCMBridge ccm;
    Cluster cluster;
    Cluster cluster2; // a second cluster to check that other clients also get notified

    // The metadatas of the two clusters (we'll test that they're kept in sync)
    List<Metadata> metadatas;

    Session session;

    @BeforeClass(groups = "short")
    public void setup() {
        ccm = CCMBridge.builder("schemaChangesTest").withNodes(1).build();

        cluster = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1)).build();
        cluster2 = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1)).build();

        metadatas = Lists.newArrayList(cluster.getMetadata(), cluster2.getMetadata());

        session = cluster.connect();
        session.execute(String.format(CREATE_KEYSPACE, "lowercase"));
        session.execute(String.format(CREATE_KEYSPACE, "\"CaseSensitive\""));
    }

    @AfterClass(groups = "short")
    public void teardown() {
        if (cluster != null)
            cluster.close();
        if (cluster2 != null)
            cluster2.close();
        if (ccm != null)
            ccm.remove();
    }


    @DataProvider(name = "existingKeyspaceName")
    public static Object[][] existingKeyspaceName() {
        return new Object[][]{ { "lowercase" }, { "\"CaseSensitive\"" } };
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

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    @CassandraVersion(major = 2.1)
    public void should_notify_of_udt_creation(String keyspace) {
        session.execute(String.format("CREATE TYPE %s.type1(i int)", keyspace));

        for (Metadata m : metadatas)
            assertThat((DataType) m.getKeyspace(keyspace).getUserType("type1"))
                .isNotNull();
    }

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    @CassandraVersion(major = 2.1)
    public void should_notify_of_udt_update(String keyspace) {
        session.execute(String.format("CREATE TYPE %s.type1(i int)", keyspace));
        session.execute(String.format("ALTER TYPE %s.type1 ADD j int", keyspace));

        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace).getUserType("type1").getFieldType("j"))
                .isNotNull();
    }

    @Test(groups = "short", dataProvider = "existingKeyspaceName")
    @CassandraVersion(major = 2.1)
    public void should_notify_of_udt_drop(String keyspace) {
        session.execute(String.format("CREATE TYPE %s.type1(i int)", keyspace));
        session.execute(String.format("DROP TYPE %s.type1", keyspace));

        for (Metadata m : metadatas)
            assertThat((DataType) m.getKeyspace(keyspace).getUserType("type1"))
                .isNull();
    }

    @DataProvider(name = "newKeyspaceName")
    public static Object[][] newKeyspaceName() {
        return new Object[][]{ { "lowercase2" }, { "\"CaseSensitive2\"" } };
    }

    @Test(groups = "short", dataProvider = "newKeyspaceName")
    public void should_notify_of_keyspace_creation(String keyspace) {
        session.execute(String.format(CREATE_KEYSPACE, keyspace));

        for (Metadata m : metadatas)
            assertThat(m.getKeyspace(keyspace))
                .isNotNull();
    }

    @Test(groups = "short", dataProvider = "newKeyspaceName")
    public void should_notify_of_keyspace_update(String keyspace) {
        session.execute(String.format(CREATE_KEYSPACE, keyspace));
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
        session.execute(String.format(CREATE_KEYSPACE, keyspace));
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
            session.executeAsync("DROP TABLE lowercase.table1"),
            session.executeAsync("DROP TABLE \"CaseSensitive\".table1"),
            session.executeAsync("DROP TYPE lowercase.type1"),
            session.executeAsync("DROP TYPE \"CaseSensitive\".type1"),
            session.executeAsync("DROP KEYSPACE lowercase2"),
            session.executeAsync("DROP KEYSPACE \"CaseSensitive2\"")
        ));
        Futures.getUnchecked(f);
    }
}
