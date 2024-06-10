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
package com.datastax.oss.driver.examples.json;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.function;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.select.Selector;

/**
 * Illustrates basic JSON support with plain JSON strings. For more advanced examples using complex
 * objects and custom codecs, refer to the other examples in this package.
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
 *   <li>creates a table "examples.querybuilder_json". If it already exists, it will be reused;
 *   <li>inserts data in the table.
 * </ul>
 *
 * @see <a href="http://www.datastax.com/dev/blog/whats-new-in-cassandra-2-2-json-support">What’s
 *     New in Cassandra 2.2: JSON Support</a>
 */
public class PlainTextJson {

  public static void main(String[] args) {

    try (CqlSession session = CqlSession.builder().build()) {
      createSchema(session);

      insertWithCoreApi(session);
      selectWithCoreApi(session);

      insertWithQueryBuilder(session);
      selectWithQueryBuilder(session);
    }
  }

  private static void createSchema(CqlSession session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS examples "
            + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    session.execute(
        "CREATE TABLE IF NOT EXISTS examples.querybuilder_json("
            + "id int PRIMARY KEY, name text, specs map<text, text>)");
  }

  /** Demonstrates data insertion with the "core" API, i.e. providing the full query strings. */
  private static void insertWithCoreApi(CqlSession session) {
    // Bind in a simple statement:
    session.execute(
        SimpleStatement.newInstance(
            "INSERT INTO examples.querybuilder_json JSON ?",
            "{ \"id\": 1, \"name\": \"Mouse\", \"specs\": { \"color\": \"silver\" } }"));

    // Bind in a prepared statement:
    // (subsequent calls to the prepare() method will return cached statement)
    PreparedStatement pst = session.prepare("INSERT INTO examples.querybuilder_json JSON :payload");
    session.execute(
        pst.bind()
            .setString(
                "payload",
                "{ \"id\": 2, \"name\": \"Keyboard\", \"specs\": { \"layout\": \"qwerty\" } }"));

    // fromJson lets you provide individual columns as JSON:
    session.execute(
        SimpleStatement.newInstance(
            "INSERT INTO examples.querybuilder_json "
                + "(id, name, specs)  VALUES (?, ?, fromJson(?))",
            3,
            "Screen",
            "{ \"size\": \"24-inch\" }"));
  }

  /** Demonstrates data retrieval with the "core" API, i.e. providing the full query strings. */
  private static void selectWithCoreApi(CqlSession session) {
    // Reading the whole row as a JSON object:
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT JSON * FROM examples.querybuilder_json WHERE id = ?", 1))
            .one();
    assert row != null;
    System.out.printf("Entry #1 as JSON: %s%n", row.getString("[json]"));

    // Extracting a particular column as JSON:
    row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT id, toJson(specs) AS json_specs FROM examples.querybuilder_json WHERE id = ?",
                    2))
            .one();
    assert row != null;
    System.out.printf(
        "Entry #%d's specs as JSON: %s%n", row.getInt("id"), row.getString("json_specs"));
  }

  /**
   * Same as {@link #insertWithCoreApi(CqlSession)}, but using {@link
   * com.datastax.oss.driver.api.querybuilder.QueryBuilder} to construct the queries.
   */
  private static void insertWithQueryBuilder(CqlSession session) {
    // Simple statement:
    Statement stmt =
        insertInto("examples", "querybuilder_json")
            .json("{ \"id\": 1, \"name\": \"Mouse\", \"specs\": { \"color\": \"silver\" } }")
            .build();
    session.execute(stmt);

    // Prepare and bind:
    PreparedStatement pst =
        session.prepare(
            insertInto("examples", "querybuilder_json").json(bindMarker("payload")).build());
    session.execute(
        pst.bind()
            .setString(
                "payload",
                "{ \"id\": 2, \"name\": \"Keyboard\", \"specs\": { \"layout\": \"qwerty\" } }"));

    // fromJson on a single column:
    stmt =
        insertInto("examples", "querybuilder_json")
            .value("id", literal(3))
            .value("name", literal("Screen"))
            .value("specs", function("fromJson", literal("{ \"size\": \"24-inch\" }")))
            .build();
    session.execute(stmt);
  }

  /**
   * Same as {@link #selectWithCoreApi(CqlSession)}, but using {@link
   * com.datastax.oss.driver.api.querybuilder.QueryBuilder} to construct the queries.
   */
  private static void selectWithQueryBuilder(CqlSession session) {
    // Reading the whole row as a JSON object:
    Statement stmt =
        selectFrom("examples", "querybuilder_json")
            .json()
            .all()
            .whereColumn("id")
            .isEqualTo(literal(1))
            .build();
    Row row = session.execute(stmt).one();
    assert row != null;
    System.out.printf("Entry #1 as JSON: %s%n", row.getString("[json]"));

    // Extracting a particular column as JSON:
    stmt =
        selectFrom("examples", "querybuilder_json")
            .column("id")
            .function("toJson", Selector.column("specs"))
            .as("json_specs")
            .whereColumn("id")
            .isEqualTo(literal(2))
            .build();

    row = session.execute(stmt).one();
    assert row != null;

    System.out.printf(
        "Entry #%d's specs as JSON: %s%n", row.getInt("id"), row.getString("json_specs"));
  }
}
