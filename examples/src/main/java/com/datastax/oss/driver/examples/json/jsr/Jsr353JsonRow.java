/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.examples.json.jsr;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.examples.json.PlainTextJson;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonStructure;

/**
 * Illustrates how to map an entire table row to a Java object using the <a
 * href="https://jcp.org/en/jsr/detail?id=353">Java API for JSON processing</a>, and leveraging the
 * {@code SELECT JSON} and {@code INSERT JSON} syntaxes introduced in Cassandra 2.2.
 *
 * <p>This example makes usage of a custom {@link TypeCodec codec}, {@link Jsr353JsonCodec}, which
 * is declared in the java-driver-examples module. If you plan to follow this example, make sure to
 * include the following Maven dependencies in your project:
 *
 * <pre>{@code
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
 *   <li>creates a table "examples.json_jsr353_row". If it already exists, it will be reused;
 *   <li>inserts data in the table.
 * </ul>
 *
 * @see <a href="http://www.datastax.com/dev/blog/whats-new-in-cassandra-2-2-json-support">What’s
 *     New in Cassandra 2.2: JSON Support</a>
 */
public class Jsr353JsonRow {

  // A codec to convert JSON payloads into JsonObject instances;
  private static final Jsr353JsonCodec USER_CODEC = new Jsr353JsonCodec();

  public static void main(String[] args) {
    try (CqlSession session = CqlSession.builder().addTypeCodecs(USER_CODEC).build()) {
      createSchema(session);
      insertJsonRow(session);
      selectJsonRow(session);
    }
  }

  private static void createSchema(CqlSession session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS examples "
            + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    session.execute(
        "CREATE TABLE IF NOT EXISTS examples.json_jsr353_row("
            + "id int PRIMARY KEY, name text, age int)");
  }

  // Mapping a User instance to a table row using INSERT JSON
  private static void insertJsonRow(CqlSession session) {

    JsonObject alice =
        Json.createObjectBuilder().add("id", 1).add("name", "alice").add("age", 30).build();

    JsonObject bob =
        Json.createObjectBuilder().add("id", 2).add("name", "bob").add("age", 35).build();

    // Build and execute a simple statement
    Statement stmt =
        insertInto("examples", "json_jsr353_row")
            .json(alice, session.getContext().getCodecRegistry())
            .build();
    session.execute(stmt);

    // The JSON object can be a bound value if the statement is prepared
    // (we use a local variable here for the sake of example, but in a real application you would
    // cache and reuse the prepared statement)
    PreparedStatement pst =
        session.prepare(insertInto("examples", "json_jsr353_row").json(bindMarker("user")).build());
    session.execute(
        pst.bind()
            // note that the codec requires that the type passed to the set() method
            // be always JsonStructure, and not a subclass of it, such as JsonObject
            .set("user", bob, JsonStructure.class));
  }

  // Retrieving User instances from table rows using SELECT JSON
  private static void selectJsonRow(CqlSession session) {

    // Reading the whole row as a JSON object
    Statement stmt =
        selectFrom("examples", "json_jsr353_row")
            .json()
            .all()
            .whereColumn("id")
            .in(literal(1), literal(2))
            .build();

    ResultSet rows = session.execute(stmt);

    for (Row row : rows) {
      // SELECT JSON returns only one column for each row, of type VARCHAR,
      // containing the row as a JSON payload.
      // Note that the codec requires that the type passed to the get() method
      // be always JsonStructure, and not a subclass of it, such as JsonObject,
      // hence the need to downcast to JsonObject manually
      JsonObject user = (JsonObject) row.get(0, JsonStructure.class);
      System.out.printf("Retrieved user: %s%n", user);
    }
  }
}
