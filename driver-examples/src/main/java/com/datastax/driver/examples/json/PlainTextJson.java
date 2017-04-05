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
package com.datastax.driver.examples.json;

import com.datastax.driver.core.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * Illustrates basic JSON support with plain JSON strings. For more advanced examples using complex objects and custom
 * codecs, refer to the other examples in this package.
 * <p/>
 * Preconditions:
 * - a Cassandra 2.2+ cluster is running and accessible through the contacts points identified by CONTACT_POINTS and
 * PORT;
 * <p/>
 * Side effects:
 * - creates a new keyspace "examples" in the cluster. If a keyspace with this name already exists, it will be reused;
 * - creates a table "examples.querybuilder_json". If it already exists, it will be reused;
 * - inserts data in the table.
 *
 * @see <a href="http://www.datastax.com/dev/blog/whats-new-in-cassandra-2-2-json-support">What’s New in Cassandra 2.2: JSON Support</a>
 */
public class PlainTextJson {

    static String[] CONTACT_POINTS = {"127.0.0.1"};
    static int PORT = 9042;

    public static void main(String[] args) {
        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                    .addContactPoints(CONTACT_POINTS).withPort(PORT)
                    .build();
            Session session = cluster.connect();

            createSchema(session);

            insertWithCoreApi(session);
            selectWithCoreApi(session);

            insertWithQueryBuilder(session);
            selectWithQueryBuilder(session);
        } finally {
            if (cluster != null) cluster.close();
        }
    }

    private static void createSchema(Session session) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS examples " +
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute("CREATE TABLE IF NOT EXISTS examples.querybuilder_json(" +
                "id int PRIMARY KEY, name text, specs map<text, text>)");
    }

    /**
     * Demonstrates data insertion with the "core" API, i.e. providing the full query strings.
     */
    private static void insertWithCoreApi(Session session) {
        // Bind in a simple statement:
        session.execute("INSERT INTO examples.querybuilder_json JSON ?",
                "{ \"id\": 1, \"name\": \"Mouse\", \"specs\": { \"color\": \"silver\" } }");

        // Bind in a prepared statement:
        // (we use a local variable here for the sake of example, but in a real application you would cache and reuse
        // the prepared statement)
        PreparedStatement pst = session.prepare("INSERT INTO examples.querybuilder_json JSON :payload");
        session.execute(pst.bind()
                .setString("payload", "{ \"id\": 2, \"name\": \"Keyboard\", \"specs\": { \"layout\": \"qwerty\" } }"));

        // fromJson lets you provide individual columns as JSON:
        session.execute("INSERT INTO examples.querybuilder_json " +
                        "(id, name, specs)  VALUES (?, ?, fromJson(?))",
                3, "Screen", "{ \"size\": \"24-inch\" }");
    }

    /**
     * Demonstrates data retrieval with the "core" API, i.e. providing the full query strings.
     */
    private static void selectWithCoreApi(Session session) {
        // Reading the whole row as a JSON object:
        Row row = session.execute("SELECT JSON * FROM examples.querybuilder_json WHERE id = ?", 1).one();
        System.out.printf("Entry #1 as JSON: %s%n", row.getString("[json]"));

        // Extracting a particular column as JSON:
        row = session.execute("SELECT id, toJson(specs) AS json_specs FROM examples.querybuilder_json WHERE id = ?", 2)
                .one();
        System.out.printf("Entry #%d's specs as JSON: %s%n",
                row.getInt("id"), row.getString("json_specs"));
    }

    /**
     * Same as {@link #insertWithCoreApi(Session)}, but using {@link com.datastax.driver.core.querybuilder.QueryBuilder}
     * to construct the queries.
     */
    private static void insertWithQueryBuilder(Session session) {
        // Simple statement:
        Statement stmt = insertInto("examples", "querybuilder_json")
                .json("{ \"id\": 1, \"name\": \"Mouse\", \"specs\": { \"color\": \"silver\" } }");
        session.execute(stmt);

        // Prepare and bind:
        // (again, cache the prepared statement in a real application)
        PreparedStatement pst = session.prepare(
                insertInto("examples", "querybuilder_json").json(bindMarker("payload")));
        session.execute(pst.bind()
                .setString("payload", "{ \"id\": 2, \"name\": \"Keyboard\", \"specs\": { \"layout\": \"qwerty\" } }"));

        // fromJson on a single column:
        stmt = insertInto("examples", "querybuilder_json")
                .value("id", 3)
                .value("name", "Screen")
                .value("specs", fromJson("{ \"size\": \"24-inch\" }"));
        session.execute(stmt);
    }

    /**
     * Same as {@link #selectWithCoreApi(Session)}, but using {@link com.datastax.driver.core.querybuilder.QueryBuilder}
     * to construct the queries.
     */
    private static void selectWithQueryBuilder(Session session) {
        // Reading the whole row as a JSON object:
        Statement stmt = select().json()
                .from("examples", "querybuilder_json")
                .where(eq("id", 1));
        Row row = session.execute(stmt).one();
        System.out.printf("Entry #1 as JSON: %s%n", row.getString("[json]"));

        // Extracting a particular column as JSON:
        stmt = select()
                .column("id")
                .toJson("specs").as("json_specs")
                .from("examples", "querybuilder_json")
                .where(eq("id", 2));
        row = session.execute(stmt).one();
        System.out.printf("Entry #%d's specs as JSON: %s%n",
                row.getInt("id"), row.getString("json_specs"));
    }
}
