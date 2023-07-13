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
package com.datastax.oss.driver.examples.basic;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Creates a keyspace and tables, and loads some data into them.
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
 *   <li>creates a new keyspace "simplex" in the session. If a keyspace with this name already
 *       exists, it will be reused;
 *   <li>creates two tables "simplex.songs" and "simplex.playlists". If they exist already, they
 *       will be reused;
 *   <li>inserts a row in each table.
 * </ul>
 *
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/4.0">Java Driver online
 *     manual</a>
 */
@SuppressWarnings("CatchAndPrintStackTrace")
public class CreateAndPopulateKeyspace {

  public static void main(String[] args) {

    CreateAndPopulateKeyspace client = new CreateAndPopulateKeyspace();

    try {
      client.connect();
      client.createSchema();
      client.loadData();
      client.querySchema();

    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      client.close();
    }
  }

  private CqlSession session;

  /** Initiates a connection to the session specified by the application.conf. */
  public void connect() {

    session = CqlSession.builder().build();

    System.out.printf("Connected session: %s%n", session.getName());
  }

  /** Creates the schema (keyspace) and tables for this example. */
  public void createSchema() {

    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS simplex WITH replication "
            + "= {'class':'SimpleStrategy', 'replication_factor':1};");

    session.execute(
        "CREATE TABLE IF NOT EXISTS simplex.songs ("
            + "id uuid PRIMARY KEY,"
            + "title text,"
            + "album text,"
            + "artist text,"
            + "tags set<text>,"
            + "data blob"
            + ");");

    session.execute(
        "CREATE TABLE IF NOT EXISTS simplex.playlists ("
            + "id uuid,"
            + "title text,"
            + "album text, "
            + "artist text,"
            + "song_id uuid,"
            + "PRIMARY KEY (id, title, album, artist)"
            + ");");
  }

  /** Inserts data into the tables. */
  public void loadData() {

    session.execute(
        "INSERT INTO simplex.songs (id, title, album, artist, tags) "
            + "VALUES ("
            + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
            + "'La Petite Tonkinoise',"
            + "'Bye Bye Blackbird',"
            + "'Joséphine Baker',"
            + "{'jazz', '2013'})"
            + ";");

    session.execute(
        "INSERT INTO simplex.playlists (id, song_id, title, album, artist) "
            + "VALUES ("
            + "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,"
            + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
            + "'La Petite Tonkinoise',"
            + "'Bye Bye Blackbird',"
            + "'Joséphine Baker'"
            + ");");
  }

  /** Queries and displays data. */
  public void querySchema() {

    ResultSet results =
        session.execute(
            "SELECT * FROM simplex.playlists "
                + "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");

    System.out.printf("%-30s\t%-20s\t%-20s%n", "title", "album", "artist");
    System.out.println(
        "-------------------------------+-----------------------+--------------------");

    for (Row row : results) {

      System.out.printf(
          "%-30s\t%-20s\t%-20s%n",
          row.getString("title"), row.getString("album"), row.getString("artist"));
    }
  }

  /** Closes the session. */
  public void close() {
    if (session != null) {
      session.close();
    }
  }
}
