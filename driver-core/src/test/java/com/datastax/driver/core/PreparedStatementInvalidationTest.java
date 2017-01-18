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


import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.DataType.cint;

@CCMConfig(clusterProvider = "createClusterBuilderNoDebouncing")
//TODO enable when CASSANDRA-10786 gets merged
@Test(enabled = false)
public class PreparedStatementInvalidationTest extends CCMTestsSupport {

    @BeforeMethod(groups = "short")
    public void createTable() throws Exception {
        execute("CREATE TABLE simpletable (a int PRIMARY KEY, b int, c int);");
    }

    @AfterMethod(groups = "short")
    public void dropTable() throws Exception {
        execute("DROP TABLE IF EXISTS simpletable");
    }

    @Test(groups = "short")
    public void should_update_statement_id_when_metadata_changed() {
        // given
        PreparedStatement ps = session().prepare("SELECT * FROM simpletable WHERE a = ?");
        MD5Digest idBefore = ps.getPreparedId().getValues().getId();
        // when
        session().execute("ALTER TABLE simpletable ADD d int");
        BoundStatement bs = ps.bind(1);
        ResultSet rows = session().execute(bs);
        // then
        MD5Digest idAfter = ps.getPreparedId().getValues().getId();
        assertThat(idBefore).isNotEqualTo(idAfter);
        assertThat(ps.getPreparedId().getValues().getResultSetMetadata())
                .hasSize(4)
                .containsVariable("d", cint());
        assertThat(bs.preparedStatement().getPreparedId().getValues().getResultSetMetadata())
                .hasSize(4)
                .containsVariable("d", cint());
        assertThat(rows.getColumnDefinitions())
                .hasSize(4)
                .containsVariable("d", cint());
    }

    @Test(groups = "short")
    public void should_update_statement_id_when_metadata_changed_across_sessions() {

        Session session1 = session();
        Session session2 = cluster().connect();
        useKeyspace(session2, keyspace);

        PreparedStatement ps1 = session1.prepare("SELECT * FROM simpletable WHERE a = ?");
        PreparedStatement ps2 = session2.prepare("SELECT * FROM simpletable WHERE a = ?");

        MD5Digest id1a = ps1.getPreparedId().getValues().getId();
        MD5Digest id2a = ps2.getPreparedId().getValues().getId();

        ResultSet rows1 = session1.execute(ps1.bind(1));
        ResultSet rows2 = session2.execute(ps2.bind(1));

        assertThat(ps1.getPreparedId().getValues().getResultSetMetadata())
                .hasSize(3)
                .containsVariable("a", cint())
                .containsVariable("b", cint())
                .containsVariable("c", cint());
        assertThat(ps2.getPreparedId().getValues().getResultSetMetadata())
                .hasSize(3)
                .containsVariable("a", cint())
                .containsVariable("b", cint())
                .containsVariable("c", cint());
        assertThat(rows1.getColumnDefinitions())
                .hasSize(3)
                .containsVariable("a", cint())
                .containsVariable("b", cint())
                .containsVariable("c", cint());
        assertThat(rows2.getColumnDefinitions())
                .hasSize(3)
                .containsVariable("a", cint())
                .containsVariable("b", cint())
                .containsVariable("c", cint());

        session1.execute("ALTER TABLE simpletable ADD d int");

        rows1 = session1.execute(ps1.bind(1));
        rows2 = session2.execute(ps2.bind(1));

        MD5Digest id1b = ps1.getPreparedId().getValues().getId();
        MD5Digest id2b = ps2.getPreparedId().getValues().getId();

        assertThat(id1a).isNotEqualTo(id1b);
        assertThat(id2a).isNotEqualTo(id2b);

        assertThat(ps1.getPreparedId().getValues().getResultSetMetadata())
                .hasSize(4)
                .containsVariable("d", cint());
        assertThat(ps2.getPreparedId().getValues().getResultSetMetadata())
                .hasSize(4)
                .containsVariable("d", cint());
        assertThat(rows1.getColumnDefinitions())
                .hasSize(4)
                .containsVariable("d", cint());
        assertThat(rows2.getColumnDefinitions())
                .hasSize(4)
                .containsVariable("d", cint());
    }

}
