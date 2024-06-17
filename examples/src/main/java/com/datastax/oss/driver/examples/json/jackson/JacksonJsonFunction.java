/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.examples.json.jackson;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.function;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.examples.json.PlainTextJson;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Illustrates how to map a single table column of an arbitrary type to a Java object using the <a
 * href="http://wiki.fasterxml.com/JacksonHome">Jackson</a> library, and leveraging the {@code
 * toJson()} and {@code fromJson()} functions introduced in Cassandra 2.2.
 *
 * <p>This example makes usage of a {@linkplain ExtraTypeCodecs#json(Class) custom codec for JSON}.
 * If you plan to follow this example, make sure to include the following Maven dependencies in your
 * project:
 *
 * <pre>{@code
 * <dependency>
 *   <groupId>com.fasterxml.jackson.core</groupId>
 *   <artifactId>jackson-databind</artifactId>
 *   <version>2.9.8</version>
 * </dependency>
 * }</pre>
 *
 * This example also uses the {@link com.datastax.oss.driver.api.querybuilder.QueryBuilder
 * QueryBuilder}; for examples using the "core" API, see {@link PlainTextJson} (they are easily
 * translatable to the queries in this class).
 *
 * <p>Preconditions:
 *
 * <ul>
 *   <li>An Apache Cassandra(R) cluster is running and accessible through the contacts points
 *       identified by basic.contact-points (see application.conf).
 * </ul>
 *
 * <p>Side effects:
 *
 * <ul>
 *   <li>creates a new keyspace "examples" in the cluster. If a keyspace with this name already
 *       exists, it will be reused;
 *   <li>creates a user-defined type (UDT) "examples.json_jackson_function_user". If it already
 *       exists, it will be reused;
 *   <li>creates a table "examples.json_jackson_function". If it already exists, it will be reused;
 *   <li>inserts data in the table.
 * </ul>
 *
 * @see <a href="http://www.datastax.com/dev/blog/whats-new-in-cassandra-2-2-json-support">What’s
 *     New in Cassandra 2.2: JSON Support</a>
 */
public class JacksonJsonFunction {

  // A codec to convert JSON payloads into User instances;
  private static final TypeCodec<User> USER_CODEC = ExtraTypeCodecs.json(User.class);

  // A codec to convert generic JSON payloads into JsonNode instances
  private static final TypeCodec<JsonNode> JSON_NODE_CODEC = ExtraTypeCodecs.json(JsonNode.class);

  public static void main(String[] args) {
    try (CqlSession session =
        CqlSession.builder().addTypeCodecs(USER_CODEC, JSON_NODE_CODEC).build()) {
      createSchema(session);
      insertFromJson(session);
      selectToJson(session);
    }
  }

  private static void createSchema(CqlSession session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS examples "
            + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    session.execute(
        "CREATE TYPE IF NOT EXISTS examples.json_jackson_function_user(" + "name text, age int)");
    session.execute(
        "CREATE TABLE IF NOT EXISTS examples.json_jackson_function("
            + "id int PRIMARY KEY, user frozen<json_jackson_function_user>, scores map<varchar,float>)");
  }

  // Mapping JSON payloads to table columns of arbitrary types,
  // using fromJson() function
  private static void insertFromJson(CqlSession session) {

    User alice = new User("alice", 30);
    User bob = new User("bob", 35);

    ObjectNode aliceScores =
        JsonNodeFactory.instance.objectNode().put("call_of_duty", 4.8).put("pokemon_go", 9.7);
    ObjectNode bobScores =
        JsonNodeFactory.instance.objectNode().put("zelda", 8.3).put("pokemon_go", 12.4);

    // Build and execute a simple statement
    Statement stmt =
        insertInto("examples", "json_jackson_function")
            .value("id", literal(1))
            // client-side, the User object will be converted into a JSON String;
            // then, server-side, the fromJson() function will convert that JSON string
            // into an instance of the json_jackson_function_user user-defined type (UDT),
            // which will be persisted into the column "user"
            .value(
                "user",
                function("fromJson", literal(alice, session.getContext().getCodecRegistry())))
            // same thing, but this time converting from
            // a generic JsonNode to a JSON string, then from this string to a map<varchar,float>
            .value(
                "scores",
                function("fromJson", literal(aliceScores, session.getContext().getCodecRegistry())))
            .build();
    System.out.println(((SimpleStatement) stmt).getQuery());
    session.execute(stmt);
    System.out.println("after");

    // The JSON object can be a bound value if the statement is prepared
    // (subsequent calls to the prepare() method will return cached statement)
    PreparedStatement pst =
        session.prepare(
            insertInto("examples", "json_jackson_function")
                .value("id", bindMarker("id"))
                .value("user", function("fromJson", bindMarker("user")))
                .value("scores", function("fromJson", bindMarker("scores")))
                .build());
    System.out.println(pst.getQuery());
    session.execute(
        pst.bind()
            .setInt("id", 2)
            .set("user", bob, User.class)
            // note that the codec requires that the type passed to the set() method
            // be always JsonNode, and not a subclass of it, such as ObjectNode
            .set("scores", bobScores, JsonNode.class));
  }

  // Retrieving JSON payloads from table columns of arbitrary types,
  // using toJson() function
  private static void selectToJson(CqlSession session) {

    Statement stmt =
        selectFrom("examples", "json_jackson_function")
            .column("id")
            .function("toJson", Selector.column("user"))
            .as("user")
            .function("toJson", Selector.column("scores"))
            .as("scores")
            .whereColumn("id")
            .in(literal(1), literal(2))
            .build();
    System.out.println(((SimpleStatement) stmt).getQuery());

    ResultSet rows = session.execute(stmt);

    for (Row row : rows) {
      int id = row.getInt("id");
      // retrieve the JSON payload and convert it to a User instance
      User user = row.get("user", User.class);
      // it is also possible to retrieve the raw JSON payload
      String userJson = row.getString("user");
      // retrieve the JSON payload and convert it to a JsonNode instance
      // note that the codec requires that the type passed to the get() method
      // be always JsonNode, and not a subclass of it, such as ObjectNode
      JsonNode scores = row.get("scores", JsonNode.class);
      // it is also possible to retrieve the raw JSON payload
      String scoresJson = row.getString("scores");
      System.out.printf(
          "Retrieved row:%n"
              + "id           %d%n"
              + "user         %s%n"
              + "user (raw)   %s%n"
              + "scores       %s%n"
              + "scores (raw) %s%n%n",
          id, user, userJson, scores, scoresJson);
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
