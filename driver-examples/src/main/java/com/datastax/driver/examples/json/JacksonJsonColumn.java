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
import com.datastax.driver.extras.codecs.json.JacksonJsonCodec;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * Illustrates how to map a single table column of type {@code VARCHAR},
 * containing JSON payloads, into a Java object using
 * the <a href="http://wiki.fasterxml.com/JacksonHome">Jackson</a> library.
 * <p/>
 * This example makes usage of a custom {@link TypeCodec codec},
 * {@link JacksonJsonCodec}, which is declared in the driver-extras module.
 * If you plan to follow this example, make sure to include the following
 * Maven dependencies in your project:
 * <pre>{@code
 * <dependency>
 *     <groupId>com.datastax.cassandra</groupId>
 *     <artifactId>cassandra-driver-extras</artifactId>
 *     <version>${driver.version}</version>
 * </dependency>
 *
 * <dependency>
 *     <groupId>com.fasterxml.jackson.core</groupId>
 *     <artifactId>jackson-databind</artifactId>
 *     <version>${jackson.version}</version>
 * </dependency>
 * }</pre>
 * This example also uses the {@link com.datastax.driver.core.querybuilder.QueryBuilder QueryBuilder};
 * for examples using the "core" API, see {@link PlainTextJson} (they are easily translatable to the
 * queries in this class).
 * <p/>
 * Preconditions:
 * - a Cassandra cluster is running and accessible through the contacts points identified by CONTACT_POINTS and PORT;
 * <p/>
 * Side effects:
 * - creates a new keyspace "examples" in the cluster. If a keyspace with this name already exists, it will be reused;
 * - creates a table "examples.json_jackson_column". If it already exists, it will be reused;
 * - inserts data in the table.
 */
public class JacksonJsonColumn {

    static String[] CONTACT_POINTS = {"127.0.0.1"};
    static int PORT = 9042;

    public static void main(String[] args) {
        Cluster cluster = null;
        try {

            // A codec to convert JSON payloads into User instances;
            // this codec is declared in the driver-extras module
            TypeCodec<User> userCodec = new JacksonJsonCodec<User>(User.class);

            cluster = Cluster.builder()
                    .addContactPoints(CONTACT_POINTS).withPort(PORT)
                    .withCodecRegistry(new CodecRegistry().register(userCodec))
                    .build();

            Session session = cluster.connect();

            createSchema(session);
            insertJsonColumn(session);
            selectJsonColumn(session);

        } finally {
            if (cluster != null) cluster.close();
        }
    }

    private static void createSchema(Session session) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS examples " +
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute("CREATE TABLE IF NOT EXISTS examples.json_jackson_column(" +
                "id int PRIMARY KEY, json text)");
    }

    // Mapping a User instance to a table column
    private static void insertJsonColumn(Session session) {

        User alice = new User("alice", 30);
        User bob = new User("bob", 35);

        // Build and execute a simple statement
        Statement stmt = insertInto("examples", "json_jackson_column")
                .value("id", 1)
                // the User object will be converted into a String and persisted into the VARCHAR column "json"
                .value("json", alice);
        session.execute(stmt);

        // The JSON object can be a bound value if the statement is prepared
        // (we use a local variable here for the sake of example, but in a real application you would cache and reuse
        // the prepared statement)
        PreparedStatement pst = session.prepare(
                insertInto("examples", "json_jackson_column")
                        .value("id", bindMarker("id"))
                        .value("json", bindMarker("json")));
        session.execute(pst.bind()
                .setInt("id", 2)
                .set("json", bob, User.class));
    }

    // Retrieving User instances from a table column
    private static void selectJsonColumn(Session session) {

        Statement stmt = select()
                .from("examples", "json_jackson_column")
                .where(in("id", 1, 2));

        ResultSet rows = session.execute(stmt);

        for (Row row : rows) {
            int id = row.getInt("id");
            // retrieve the JSON payload and convert it to a User instance
            User user = row.get("json", User.class);
            // it is also possible to retrieve the raw JSON payload
            String json = row.getString("json");
            System.out.printf("Retrieved row:%n" +
                            "id           %d%n" +
                            "user         %s%n" +
                            "user (raw)   %s%n%n",
                    id, user, json);

        }
    }

    @SuppressWarnings("unused")
    public static class User {

        private final String name;

        private final int age;

        @JsonCreator
        public User(@JsonProperty("name") String name, @JsonProperty("age") int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }

        @Override
        public String toString() {
            return String.format("%s (%s)", name, age);
        }
    }
}
