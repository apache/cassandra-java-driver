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
package com.datastax.oss.driver.examples.datatypes;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

/**
 * Inserts and retrieves values in columns of user-defined types.
 *
 * <p>By default, the Java driver maps user-defined types to {@link UdtValue}. This example shows
 * how to create instances of {@link UdtValue}, how to insert them in the database, and how to
 * retrieve such instances from the database.
 *
 * <p>For a more complex example showing how to map user-defined types to arbitrary Java types, see
 * {@link UserDefinedTypesMapped}.
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
 *   <li>creates a table "examples.udts". If it already exists, it will be reused;
 *   <li>inserts data in the table.
 * </ul>
 *
 * @see <a
 *     href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/custom_codecs/">driver
 *     documentation on custom codecs</a>
 * @see UserDefinedTypesMapped
 */
public class UserDefinedTypesSimple {

  public static void main(String[] args) {
    try (CqlSession session = CqlSession.builder().build()) {
      createSchema(session);
      insertData(session);
      retrieveData(session);
    }
  }

  private static void createSchema(CqlSession session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS examples "
            + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    session.execute("CREATE TYPE IF NOT EXISTS examples.coordinates(x int, y int)");
    session.execute("CREATE TABLE IF NOT EXISTS examples.udts(k int PRIMARY KEY, c coordinates)");
  }

  private static void insertData(CqlSession session) {
    // prepare the INSERT statement
    PreparedStatement prepared = session.prepare("INSERT INTO examples.udts (k, c) VALUES (?, ?)");

    // retrieve the user-defined type metadata
    UserDefinedType coordinatesType = retrieveCoordinatesType(session);

    // bind the parameters in one pass
    UdtValue coordinates1 = coordinatesType.newValue(12, 34);
    BoundStatement boundStatement1 = prepared.bind(1, coordinates1);
    // execute the insertion
    session.execute(boundStatement1);

    // alternate method: bind the parameters one by one
    UdtValue coordinates2 = coordinatesType.newValue(56, 78);
    BoundStatement boundStatement2 = prepared.bind().setInt("k", 2).setUdtValue("c", coordinates2);
    // execute the insertion
    session.execute(boundStatement2);
  }

  private static void retrieveData(CqlSession session) {
    for (int k = 1; k <= 2; k++) {
      // Execute the SELECT query and retrieve the single row in the result set
      SimpleStatement statement =
          SimpleStatement.newInstance("SELECT c FROM examples.udts WHERE k = ?", k);
      Row row = session.execute(statement).one();
      assert row != null;

      // Retrieve the value for column c
      UdtValue coordinatesValue = row.getUdtValue("c");
      assert coordinatesValue != null;

      // Display the contents of the UdtValue instance
      System.out.printf(
          "found coordinate: (%d,%d)%n",
          coordinatesValue.getInt("x"), coordinatesValue.getInt("y"));
    }
  }

  private static UserDefinedType retrieveCoordinatesType(CqlSession session) {
    return session
        .getMetadata()
        .getKeyspace("examples")
        .flatMap(ks -> ks.getUserDefinedType("coordinates"))
        .orElseThrow(IllegalStateException::new);
  }
}
