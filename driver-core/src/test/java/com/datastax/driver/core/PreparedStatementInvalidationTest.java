
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


import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static junit.framework.TestCase.fail;

/**
 * Note: at the time of writing, this test exercises features of an unreleased Cassandra version. To
 * test against a local build, run with
 *
 * <pre>
 *   -Dcassandra.version=4.0.0 -Dcassandra.directory=/path/to/cassandra
 * </pre>
 */
@CassandraVersion("4.0")
public class PreparedStatementInvalidationTest extends CCMTestsSupport {

    @Override
    public Cluster.Builder createClusterBuilder() {
        // TODO remove when protocol v5 is stable in C* 4
        return super.createClusterBuilderNoDebouncing().allowBetaProtocolVersion();
    }

    @BeforeMethod(groups = "short", alwaysRun = true)
    public void setup() throws Exception {
        execute("CREATE TABLE prepared_statement_invalidation_test (a int PRIMARY KEY, b int, c int);");
        execute("INSERT INTO prepared_statement_invalidation_test (a, b, c) VALUES (1, 1, 1);");
        execute("INSERT INTO prepared_statement_invalidation_test (a, b, c) VALUES (2, 2, 2);");
        execute("INSERT INTO prepared_statement_invalidation_test (a, b, c) VALUES (3, 3, 3);");
        execute("INSERT INTO prepared_statement_invalidation_test (a, b, c) VALUES (4, 4, 4);");
    }

    @AfterMethod(groups = "short", alwaysRun = true)
    public void teardown() throws Exception {
        execute("DROP TABLE prepared_statement_invalidation_test");
    }

    @Test(groups = "short")
    public void should_update_statement_id_when_metadata_changed_across_executions() {
        // given
        PreparedStatement ps = session().prepare("SELECT * FROM prepared_statement_invalidation_test WHERE a = ?");
        MD5Digest idBefore = ps.getPreparedId().resultSetMetadata.id;
        // when
        session().execute("ALTER TABLE prepared_statement_invalidation_test ADD d int");
        BoundStatement bs = ps.bind(1);
        ResultSet rows = session().execute(bs);
        // then
        MD5Digest idAfter = ps.getPreparedId().resultSetMetadata.id;
        assertThat(idBefore).isNotEqualTo(idAfter);
        assertThat(ps.getPreparedId().resultSetMetadata.variables)
                .hasSize(4)
                .containsVariable("d", DataType.cint());
        assertThat(bs.preparedStatement().getPreparedId().resultSetMetadata.variables)
                .hasSize(4)
                .containsVariable("d", DataType.cint());
        assertThat(rows.getColumnDefinitions())
                .hasSize(4)
                .containsVariable("d", DataType.cint());
    }

    @Test(groups = "short")
    public void should_update_statement_id_when_metadata_changed_across_pages() throws Exception {
        // given
        PreparedStatement ps = session().prepare("SELECT * FROM prepared_statement_invalidation_test");
        ResultSet rows = session().execute(ps.bind().setFetchSize(2));
        assertThat(rows.isFullyFetched()).isFalse();
        MD5Digest idBefore = ps.getPreparedId().resultSetMetadata.id;
        ColumnDefinitions definitionsBefore = rows.getColumnDefinitions();
        assertThat(definitionsBefore)
                .hasSize(3)
                .doesNotContainVariable("d");
        // consume the first page
        int remaining = rows.getAvailableWithoutFetching();
        while (remaining-- > 0) {
            try {
                rows.one().getInt("d");
                fail("expected an error");
            } catch (IllegalArgumentException e) { /*expected*/ }
        }

        // when
        session().execute("ALTER TABLE prepared_statement_invalidation_test ADD d int");

        // then
        // this should trigger a background fetch of the second page, and therefore update the definitions
        for (Row row : rows) {
            assertThat(row.isNull("d")).isTrue();
        }
        MD5Digest idAfter = ps.getPreparedId().resultSetMetadata.id;
        ColumnDefinitions definitionsAfter = rows.getColumnDefinitions();
        assertThat(idBefore).isNotEqualTo(idAfter);
        assertThat(definitionsAfter)
                .hasSize(4)
                .containsVariable("d", DataType.cint());
    }

    @Test(groups = "short")
    public void should_update_statement_id_when_metadata_changed_across_sessions() {
        Session session1 = cluster().connect();
        useKeyspace(session1, keyspace);
        Session session2 = cluster().connect();
        useKeyspace(session2, keyspace);

        PreparedStatement ps1 = session1.prepare("SELECT * FROM prepared_statement_invalidation_test WHERE a = ?");
        PreparedStatement ps2 = session2.prepare("SELECT * FROM prepared_statement_invalidation_test WHERE a = ?");

        MD5Digest id1a = ps1.getPreparedId().resultSetMetadata.id;
        MD5Digest id2a = ps2.getPreparedId().resultSetMetadata.id;

        ResultSet rows1 = session1.execute(ps1.bind(1));
        ResultSet rows2 = session2.execute(ps2.bind(1));

        assertThat(rows1.getColumnDefinitions())
                .hasSize(3)
                .containsVariable("a", DataType.cint())
                .containsVariable("b", DataType.cint())
                .containsVariable("c", DataType.cint());
        assertThat(rows2.getColumnDefinitions())
                .hasSize(3)
                .containsVariable("a", DataType.cint())
                .containsVariable("b", DataType.cint())
                .containsVariable("c", DataType.cint());

        session1.execute("ALTER TABLE prepared_statement_invalidation_test ADD d int");

        rows1 = session1.execute(ps1.bind(1));
        rows2 = session2.execute(ps2.bind(1));

        MD5Digest id1b = ps1.getPreparedId().resultSetMetadata.id;
        MD5Digest id2b = ps2.getPreparedId().resultSetMetadata.id;

        assertThat(id1a).isNotEqualTo(id1b);
        assertThat(id2a).isNotEqualTo(id2b);

        assertThat(ps1.getPreparedId().resultSetMetadata.variables)
                .hasSize(4)
                .containsVariable("d", DataType.cint());
        assertThat(ps2.getPreparedId().resultSetMetadata.variables)
                .hasSize(4)
                .containsVariable("d", DataType.cint());
        assertThat(rows1.getColumnDefinitions())
                .hasSize(4)
                .containsVariable("d", DataType.cint());
        assertThat(rows2.getColumnDefinitions())
                .hasSize(4)
                .containsVariable("d", DataType.cint());
    }

    @Test(groups = "short", expectedExceptions = NoHostAvailableException.class)
    public void should_not_reprepare_invalid_statements() {
        // given
        session().execute("ALTER TABLE prepared_statement_invalidation_test ADD d int");
        PreparedStatement ps = session().prepare("SELECT a, b, c, d FROM prepared_statement_invalidation_test WHERE a = ?");
        session().execute("ALTER TABLE prepared_statement_invalidation_test DROP d");
        // when
        session().execute(ps.bind());
    }

    @Test(groups = "short")
    public void should_never_update_statement_id_for_conditional_updates_in_modern_protocol() {
        should_never_update_statement_id_for_conditional_updates(session());
    }

    private void should_never_update_statement_id_for_conditional_updates(Session session) {
        // Given
        PreparedStatement ps = session.prepare(
                "INSERT INTO prepared_statement_invalidation_test (a, b, c) VALUES (?, ?, ?) IF NOT EXISTS");

        // Never store metadata in the prepared statement for conditional updates, since the result set can change
        // depending on the outcome.
        assertThat(ps.getPreparedId().resultSetMetadata.variables).isNull();
        MD5Digest idBefore = ps.getPreparedId().resultSetMetadata.id;

        // When
        ResultSet rs = session.execute(ps.bind(5, 5, 5));

        // Then
        // Successful conditional update => only contains the [applied] column
        assertThat(rs.wasApplied()).isTrue();
        assertThat(rs.getColumnDefinitions())
                .hasSize(1)
                .containsVariable("[applied]", DataType.cboolean());
        // However the prepared statement shouldn't have changed
        assertThat(ps.getPreparedId().resultSetMetadata.variables).isNull();
        assertThat(ps.getPreparedId().resultSetMetadata.id).isEqualTo(idBefore);


        // When
        rs = session.execute(ps.bind(5, 5, 5));

        // Then
        // Failed conditional update => regular metadata
        assertThat(rs.wasApplied()).isFalse();
        assertThat(rs.getColumnDefinitions()).hasSize(4);
        Row row = rs.one();
        assertThat(row.getBool("[applied]")).isFalse();
        assertThat(row.getInt("a")).isEqualTo(5);
        assertThat(row.getInt("b")).isEqualTo(5);
        assertThat(row.getInt("c")).isEqualTo(5);
        // The prepared statement still shouldn't have changed
        assertThat(ps.getPreparedId().resultSetMetadata.variables).isNull();
        assertThat(ps.getPreparedId().resultSetMetadata.id).isEqualTo(idBefore);


        // When
        session.execute("ALTER TABLE prepared_statement_invalidation_test ADD d int");
        rs = session.execute(ps.bind(5, 5, 5));

        // Then
        // Failed conditional update => regular metadata that should also contain the new column
        assertThat(rs.wasApplied()).isFalse();
        assertThat(rs.getColumnDefinitions()).hasSize(5);
        row = rs.one();
        assertThat(row.getBool("[applied]")).isFalse();
        assertThat(row.getInt("a")).isEqualTo(5);
        assertThat(row.getInt("b")).isEqualTo(5);
        assertThat(row.getInt("c")).isEqualTo(5);
        assertThat(row.isNull("d")).isTrue();
        assertThat(ps.getPreparedId().resultSetMetadata.variables).isNull();
        assertThat(ps.getPreparedId().resultSetMetadata.id).isEqualTo(idBefore);
    }

    @Test(groups = "short")
    public void should_never_update_statement_for_conditional_updates_in_legacy_protocols() {
        // Given
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withProtocolVersion(ccm().getProtocolVersion(ProtocolVersion.V4))
                .build());
        Session session = cluster.connect(keyspace);
        should_never_update_statement_id_for_conditional_updates(session);
    }
}
