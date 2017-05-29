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
package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.testng.Assert.*;

@CCMConfig(clusterProvider = "createClusterBuilderNoDebouncing")
public class QueryBuilderITest extends CCMTestsSupport {

    private static final String TABLE_TEXT = "test_text";
    private static final String TABLE_INT = "test_int";

    @Override
    public void onTestContextInitialized() {
        execute(String.format("CREATE TABLE %s (k text PRIMARY KEY, a int, b int)", TABLE_TEXT),
                String.format("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)", TABLE_INT));
    }

    @Test(groups = "short")
    public void remainingDeleteTests() throws Exception {

        Statement query;
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable(TABLE_TEXT);
        assertNotNull(table);

        String expected = String.format("DELETE k FROM %s.test_text;", keyspace);
        query = delete("k").from(table);
        assertEquals(query.toString(), expected);
        try {
            session().execute(query);
            fail();
        } catch (SyntaxError e) {
            // Missing WHERE clause
        }
    }

    @Test(groups = "short")
    public void selectInjectionTests() throws Exception {

        String query;
        Statement select;
        PreparedStatement ps;
        BoundStatement bs;

        session().execute("CREATE TABLE foo ( k ascii PRIMARY KEY , i int, s ascii )");

        query = "SELECT * FROM foo WHERE k=?;";
        select = select().all().from("foo").where(eq("k", bindMarker()));
        ps = session().prepare(select.toString());
        bs = ps.bind();
        assertEquals(select.toString(), query);
        session().execute(bs.setString("k", "4 AND c=5"));
    }

    @Test(groups = "short")
    @CassandraVersion(value = "2.0.7", description = "DELETE..IF EXISTS only supported in 2.0.7+ (CASSANDRA-5708)")
    public void conditionalDeletesTest() throws Exception {
        session().execute(String.format("INSERT INTO %s.test_int (k, a, b) VALUES (1, 1, 1)", keyspace));

        Statement delete;
        Row row;
        delete = delete().from(keyspace, TABLE_INT).where(eq("k", 2)).ifExists();
        row = session().execute(delete).one();
        assertFalse(row.getBool("[applied]"));

        delete = delete().from(keyspace, TABLE_INT).where(eq("k", 1)).ifExists();
        row = session().execute(delete).one();
        assertTrue(row.getBool("[applied]"));

        session().execute(String.format("INSERT INTO %s.test_int (k, a, b) VALUES (1, 1, 1)", keyspace));

        delete = delete().from(keyspace, TABLE_INT).where(eq("k", 1)).onlyIf(eq("a", 1)).and(eq("b", 2));
        row = session().execute(delete).one();
        assertFalse(row.getBool("[applied]"));

        delete = delete().from(keyspace, TABLE_INT).where(eq("k", 1)).onlyIf(eq("a", 1)).and(eq("b", 1));
        row = session().execute(delete).one();
        assertTrue(row.getBool("[applied]"));
    }

    @Test(groups = "short")
    @CassandraVersion(value = "2.0.13", description = "Allow IF EXISTS for UPDATE statements (CASSANDRA-8610)")
    public void conditionalUpdatesTest() throws Exception {
        session().execute(String.format("INSERT INTO %s.test_int (k, a, b) VALUES (1, 1, 1)", keyspace));

        Statement update;
        Row row;
        update = update(TABLE_INT).with(set("a", 2)).and(set("b", 2)).where(eq("k", 2)).ifExists();
        row = session().execute(update).one();
        assertFalse(row.getBool("[applied]"));

        update = update(TABLE_INT).with(set("a", 2)).and(set("b", 2)).where(eq("k", 1)).ifExists();
        row = session().execute(update).one();
        assertTrue(row.getBool("[applied]"));

        update = update(TABLE_INT).with(set("a", 2)).and(set("b", 2)).where(eq("k", 2)).onlyIf(eq("a", 1)).and(eq("b", 2));
        row = session().execute(update).one();
        assertFalse(row.getBool("[applied]"));

        update = update(TABLE_INT).with(set("a", 3)).and(set("b", 3)).where(eq("k", 1)).onlyIf(eq("a", 2)).and(eq("b", 2));
        row = session().execute(update).one();
        assertTrue(row.getBool("[applied]"));

        update = update(TABLE_INT).with(set("a", 4)).and(set("b", 4)).onlyIf(eq("a", 2)).and(eq("b", 2)).where(eq("k", 1));
        row = session().execute(update).one();
        assertFalse(row.getBool("[applied]"));

        update = update(TABLE_INT).with(set("a", 4)).and(set("b", 4)).onlyIf(eq("a", 3)).and(eq("b", 3)).where(eq("k", 1));
        row = session().execute(update).one();
        assertTrue(row.getBool("[applied]"));
    }
}
