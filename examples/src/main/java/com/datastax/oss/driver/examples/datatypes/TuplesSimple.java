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
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;

/**
 * Inserts and retrieves values in columns of tuple types.
 *
 * <p>By default, the Java driver maps tuples to {@link TupleValue}. This example shows how to
 * create instances of {@link TupleValue}, how to insert them in the database, and how to retrieve
 * such instances from the database.
 *
 * <p>For a more complex example showing how to map tuples to arbitrary Java types, see {@link
 * TuplesMapped}.
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
 *   <li>creates a table "examples.tuples". If it already exists, it will be reused;
 *   <li>inserts data in the table.
 * </ul>
 *
 * @see <a
 *     href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/custom_codecs/">driver
 *     documentation on custom codecs</a>
 * @see TuplesMapped
 */
public class TuplesSimple {

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
    session.execute(
        "CREATE TABLE IF NOT EXISTS examples.tuples(k int PRIMARY KEY, c tuple<int,int>)");
  }

  private static void insertData(CqlSession session) {
    // prepare the INSERT statement
    PreparedStatement prepared =
        session.prepare("INSERT INTO examples.tuples (k, c) VALUES (?, ?)");

    // create the tuple metadata
    TupleType coordinatesType = DataTypes.tupleOf(DataTypes.INT, DataTypes.INT);

    // bind the parameters in one pass
    TupleValue coordinates1 = coordinatesType.newValue(12, 34);
    BoundStatement boundStatement1 = prepared.bind(1, coordinates1);
    // execute the insertion
    session.execute(boundStatement1);

    // alternate method: bind the parameters one by one
    TupleValue coordinates2 = coordinatesType.newValue(56, 78);
    BoundStatement boundStatement2 =
        prepared.bind().setInt("k", 2).setTupleValue("c", coordinates2);
    // execute the insertion
    session.execute(boundStatement2);
  }

  private static void retrieveData(CqlSession session) {
    for (int k = 1; k <= 2; k++) {
      // Execute the SELECT query and retrieve the single row in the result set
      SimpleStatement statement =
          SimpleStatement.newInstance("SELECT c FROM examples.tuples WHERE k = ?", k);
      Row row = session.execute(statement).one();
      assert row != null;

      // Retrieve the value for column c
      TupleValue coordinatesValue = row.getTupleValue("c");
      assert coordinatesValue != null;

      // Display the contents of the tuple
      System.out.printf(
          "found coordinate: (%d,%d)%n", coordinatesValue.getInt(0), coordinatesValue.getInt(1));
    }
  }
}
