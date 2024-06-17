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
package com.datastax.oss.driver.examples.json.jsr;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.function;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.examples.json.PlainTextJson;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonStructure;

/**
 * Illustrates how to map a single table column of an arbitrary type to a Java object using the <a
 * href="https://jcp.org/en/jsr/detail?id=353">Java API for JSON processing</a>, and leveraging the
 * {@code toJson()} and {@code fromJson()} functions introduced in Cassandra 2.2.
 *
 * <p>This example makes usage of a custom {@link TypeCodec codec}, {@link Jsr353JsonCodec}, which
 * is declared in the java-driver-examples module. If you plan to follow this example, make sure to
 * include the following Maven dependencies in your project:
 *
 * <pre>{
 * <dependency>
 *     <groupId>javax.json</groupId>
 *     <artifactId>javax.json-api</artifactId>
 *     <version>1.0</version>
 * </dependency>
 *
 * <dependency>
 *     <groupId>org.glassfish</groupId>
 *     <artifactId>javax.json</artifactId>
 *     <version>1.1.4</version>
 *     <scope>runtime</scope>
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
 *   <li>creates a user-defined type (UDT) "examples.json_jsr353_function_user". If it already
 *       exists, it will be reused;
 *   <li>creates a table "examples.json_jsr353_function". If it already exists, it will be reused;
 *   <li>inserts data in the table.
 * </ul>
 *
 * @see <a href="http://www.datastax.com/dev/blog/whats-new-in-cassandra-2-2-json-support">What’s
 *     New in Cassandra 2.2: JSON Support</a>
 */
public class Jsr353JsonFunction {

  // A codec to convert JSON payloads into JsonObject instances;
  private static final Jsr353JsonCodec USER_CODEC = new Jsr353JsonCodec();

  public static void main(String[] args) {
    try (CqlSession session = CqlSession.builder().addTypeCodecs(USER_CODEC).build()) {

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
        "CREATE TYPE IF NOT EXISTS examples.json_jsr353_function_user(name text, age int)");
    session.execute(
        "CREATE TABLE IF NOT EXISTS examples.json_jsr353_function("
            + "id int PRIMARY KEY, user frozen<json_jsr353_function_user>, scores map<varchar,float>)");
  }

  // Mapping JSON payloads to table columns of arbitrary types,
  // using fromJson() function
  private static void insertFromJson(CqlSession session) {

    JsonObject alice = Json.createObjectBuilder().add("name", "alice").add("age", 30).build();

    JsonObject bob = Json.createObjectBuilder().add("name", "bob").add("age", 35).build();

    JsonObject aliceScores =
        Json.createObjectBuilder().add("call_of_duty", 4.8).add("pokemon_go", 9.7).build();

    JsonObject bobScores =
        Json.createObjectBuilder().add("zelda", 8.3).add("pokemon_go", 12.4).build();

    // Build and execute a simple statement
    Statement stmt =
        insertInto("examples", "json_jsr353_function")
            .value("id", literal(1))
            // client-side, the JsonObject will be converted into a JSON String;
            // then, server-side, the fromJson() function will convert that JSON string
            // into an instance of the json_jsr353_function_user user-defined type (UDT),
            // which will be persisted into the column "user"
            .value(
                "user",
                function("fromJson", literal(alice, session.getContext().getCodecRegistry())))
            // same thing, but this time converting from
            // a JsonObject to a JSON string, then from this string to a map<varchar,float>
            .value(
                "scores",
                function("fromJson", literal(aliceScores, session.getContext().getCodecRegistry())))
            .build();
    session.execute(stmt);

    // The JSON object can be a bound value if the statement is prepared
    // (subsequent calls to the prepare() method will return cached statement)
    PreparedStatement pst =
        session.prepare(
            insertInto("examples", "json_jsr353_function")
                .value("id", bindMarker("id"))
                .value("user", function("fromJson", bindMarker("user")))
                .value("scores", function("fromJson", bindMarker("scores")))
                .build());
    session.execute(
        pst.bind()
            .setInt("id", 2)
            // note that the codec requires that the type passed to the set() method
            // be always JsonStructure, and not a subclass of it, such as JsonObject
            .set("user", bob, JsonStructure.class)
            .set("scores", bobScores, JsonStructure.class));
  }

  // Retrieving JSON payloads from table columns of arbitrary types,
  // using toJson() function
  private static void selectToJson(CqlSession session) {

    Statement stmt =
        selectFrom("examples", "json_jsr353_function")
            .column("id")
            .function("toJson", Selector.column("user"))
            .as("user")
            .function("toJson", Selector.column("scores"))
            .as("scores")
            .whereColumn("id")
            .in(literal(1), literal(2))
            .build();

    ResultSet rows = session.execute(stmt);

    for (Row row : rows) {
      int id = row.getInt("id");
      // retrieve the JSON payload and convert it to a JsonObject instance
      // note that the codec requires that the type passed to the get() method
      // be always JsonStructure, and not a subclass of it, such as JsonObject,
      // hence the need to downcast to JsonObject manually
      JsonObject user = (JsonObject) row.get("user", JsonStructure.class);
      // it is also possible to retrieve the raw JSON payload
      String userJson = row.getString("user");
      // retrieve the JSON payload and convert it to a JsonObject instance
      JsonObject scores = (JsonObject) row.get("scores", JsonStructure.class);
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
}
