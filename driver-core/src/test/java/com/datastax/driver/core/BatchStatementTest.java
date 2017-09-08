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

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.utils.CassandraVersion;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@CassandraVersion("2.0.0")
public class BatchStatementTest extends CCMTestsSupport {

    private static final String keyspace2 = TestUtils.generateIdentifier("ks_");

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE test (k text, v int, PRIMARY KEY (k, v))",
                "CREATE TABLE test2 (k text, v int, PRIMARY KEY (k, v))",
                String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace2, 1),
                String.format("CREATE TABLE %s.users(id int, id2 int, name text, primary key (id, id2))", keyspace2)
        );
    }

    @Test(groups = "short")
    public void simpleBatchTest() {
        try {
            PreparedStatement st = session().prepare("INSERT INTO test (k, v) VALUES (?, ?)");

            BatchStatement batch = new BatchStatement();

            batch.add(new SimpleStatement("INSERT INTO test (k, v) VALUES (?, ?)", "key1", 0));
            batch.add(st.bind("key1", 1));
            batch.add(st.bind("key2", 0));

            assertEquals(3, batch.size());

            session().execute(batch);

            ResultSet rs = session().execute("SELECT * FROM test");

            Row r;

            r = rs.one();
            assertEquals(r.getString("k"), "key1");
            assertEquals(r.getInt("v"), 0);

            r = rs.one();
            assertEquals(r.getString("k"), "key1");
            assertEquals(r.getInt("v"), 1);

            r = rs.one();
            assertEquals(r.getString("k"), "key2");
            assertEquals(r.getInt("v"), 0);

            assertTrue(rs.isExhausted());
        } catch (UnsupportedFeatureException e) {
            // This is expected when testing the protocol v1
            assertEquals(cluster().getConfiguration().getProtocolOptions().getProtocolVersion(), ProtocolVersion.V1);
        }
    }

    @Test(groups = "short")
    @CassandraVersion(value = "2.0.9", description = "This will only work with C* 2.0.9 (CASSANDRA-7337)")
    public void casBatchTest() {
        PreparedStatement st = session().prepare("INSERT INTO test2 (k, v) VALUES (?, ?) IF NOT EXISTS");

        BatchStatement batch = new BatchStatement();

        batch.add(new SimpleStatement("INSERT INTO test2 (k, v) VALUES (?, ?)", "key1", 0));
        batch.add(st.bind("key1", 1));
        batch.add(st.bind("key1", 2));

        assertEquals(3, batch.size());

        ResultSet rs = session().execute(batch);
        Row r = rs.one();
        assertTrue(!r.isNull("[applied]"));
        assertEquals(r.getBool("[applied]"), true);

        rs = session().execute(batch);
        r = rs.one();
        assertTrue(!r.isNull("[applied]"));
        assertEquals(r.getBool("[applied]"), false);
    }

    @CassandraVersion("4.0.0")
    @Test(groups = "short")
    public void should_use_keyspace_if_set_on_batch_statement() {
        insertWithKeyspaceOnStatement(keyspace);
    }

    @CassandraVersion("4.0.0")
    @Test(groups = "short")
    public void should_use_keyspace_if_set_on_batch_statement_no_ks_on_session() {
        insertWithKeyspaceOnStatement(null);
    }

    @CassandraVersion("4.0.0")
    @Test(groups = "short")
    public void should_use_keyspace_if_set_on_batch_statement_same_ks_on_session() {
        insertWithKeyspaceOnStatement(keyspace2);
    }

    @Test(groups = "short", expectedExceptions = {InvalidQueryException.class})
    public void should_not_use_keyspace_if_set_and_protocol_does_not_support() {
        Cluster cluster = cluster();
        if (cluster().getConfiguration().getProtocolOptions().getProtocolVersion().compareTo(ProtocolVersion.V5) >= 0) {
            // Downgrade to V4
            cluster = createClusterBuilderNoDebouncing().addContactPointsWithPorts(getContactPointsWithPorts())
                    .withNettyOptions(TestUtils.nonQuietClusterCloseOptions)
                    .withProtocolVersion(ProtocolVersion.V4).build();
        }
        insertWithKeyspaceOnStatement(cluster, keyspace);
    }

    @CassandraVersion("4.0.0")
    @Test(groups = "short")
    public void should_inherit_keyspace_from_inner_statement() {
        // TODO, reuse cluster when Protocol V5 is no longer beta.
        Cluster cluster = createClusterBuilderNoDebouncing()
                .withNettyOptions(TestUtils.nonQuietClusterCloseOptions)
                .addContactPointsWithPorts(getContactPointsWithPorts())
                .allowBetaProtocolVersion()
                .build();
        try {
            Session session = cluster.connect();
            int id = idCounter.incrementAndGet();

            BatchStatement batch = new BatchStatement();
            batch.add(new SimpleStatement("INSERT INTO users (id, id2, name) values (?, 1, 'hello')", id));
            batch.add(new SimpleStatement("INSERT INTO users (id, id2, name) values (?, 2, 'sweet')", id).setKeyspace(keyspace2));
            batch.add(new SimpleStatement("INSERT INTO users (id, id2, name) values (?, 3, 'world')", id));
            // keyspace should be inherited from first statement that has one.
            assertThat(batch.getKeyspace()).isEqualTo(keyspace2);

            session.execute(batch);

            validateData(session, id);
        } finally {
            cluster.close();
        }
    }

    private void insertWithKeyspaceOnStatement(String sessionKeyspace) {
        // TODO, reuse cluster when Protocol V5 is no longer beta.
        Cluster cluster = createClusterBuilderNoDebouncing()
                .withNettyOptions(TestUtils.nonQuietClusterCloseOptions)
                .addContactPointsWithPorts(getContactPointsWithPorts())
                .allowBetaProtocolVersion()
                .build();
        insertWithKeyspaceOnStatement(cluster, sessionKeyspace);
    }

    private static final AtomicInteger idCounter = new AtomicInteger(0);

    private void insertWithKeyspaceOnStatement(Cluster cluster, String sessionKeyspace) {
        Session session;
        if (sessionKeyspace != null) {
            session = cluster.connect(sessionKeyspace);
        } else {
            session = cluster.connect();
        }
        try {
            int id = idCounter.incrementAndGet();

            BatchStatement batch = new BatchStatement();
            batch.add(new SimpleStatement("INSERT INTO users (id, id2, name) values (?, 1, 'hello')", id));
            batch.add(new SimpleStatement("INSERT INTO users (id, id2, name) values (?, 2, 'sweet')", id));
            batch.add(new SimpleStatement("INSERT INTO users (id, id2, name) values (?, 3, 'world')", id));
            batch.setKeyspace(keyspace2);
            assertThat(batch.getKeyspace()).isEqualTo(keyspace2);

            session.execute(batch);

            validateData(session, id);
        } finally {
            session.close();
            if (cluster != cluster()) {
                cluster.close();
            }
        }
    }

    private void validateData(Session session, int id) {
        ResultSet result = session.execute(new SimpleStatement("select * from users where id = ?", id).setKeyspace(keyspace2));

        assertThat(result.getAvailableWithoutFetching()).isEqualTo(3);
        Row row1 = result.one();
        assertThat(row1.getInt("id")).isEqualTo(id);
        assertThat(row1.getInt("id2")).isEqualTo(1);
        assertThat(row1.getString("name")).isEqualTo("hello");
        Row row2 = result.one();
        assertThat(row2.getInt("id")).isEqualTo(id);
        assertThat(row2.getInt("id2")).isEqualTo(2);
        assertThat(row2.getString("name")).isEqualTo("sweet");
        Row row3 = result.one();
        assertThat(row3.getInt("id")).isEqualTo(id);
        assertThat(row3.getInt("id2")).isEqualTo(3);
        assertThat(row3.getString("name")).isEqualTo("world");
    }
}
