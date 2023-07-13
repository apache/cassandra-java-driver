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
package com.datastax.oss.driver.examples.datatypes;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;

/**
 * Inserts and retrieves values in columns of tuples.
 *
 * <p>By default, the Java Driver maps tuples to {@link TupleValue}. This example goes beyond that
 * and shows how to map tuples to arbitrary Java types, leveraging the special {@link MappingCodec}.
 *
 * <p>A simpler example of usage of tuples can be found in {@link TuplesSimple}.
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
 * @see TuplesSimple
 * @see MappingCodec
 * @see <a
 *     href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/custom_codecs/">driver
 *     documentation on custom codecs</a>
 */
public class TuplesMapped {

  /** The Java Pojo that will be mapped to the tuple "coordinates". */
  public static class Coordinates {

    private final int x;
    private final int y;

    public Coordinates(int x, int y) {
      this.x = x;
      this.y = y;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof Coordinates)) {
        return false;
      } else {
        Coordinates that = (Coordinates) o;
        return x == that.x && y == that.y;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(x, y);
    }

    @Override
    public String toString() {
      return "(" + x + ',' + y + ')';
    }
  }

  /** The custom codec that will convert to and from {@link Coordinates}. */
  public static class CoordinatesCodec extends MappingCodec<TupleValue, Coordinates> {

    public CoordinatesCodec(@NonNull TypeCodec<TupleValue> innerCodec) {
      super(innerCodec, GenericType.of(Coordinates.class));
    }

    @NonNull
    @Override
    public TupleType getCqlType() {
      return (TupleType) super.getCqlType();
    }

    @Nullable
    @Override
    protected Coordinates innerToOuter(@Nullable TupleValue value) {
      return value == null ? null : new Coordinates(value.getInt(0), value.getInt(1));
    }

    @Nullable
    @Override
    protected TupleValue outerToInner(@Nullable Coordinates value) {
      return value == null
          ? null
          : this.getCqlType().newValue().setInt(0, value.x).setInt(1, value.y);
    }
  }

  public static void main(String[] args) {
    try (CqlSession session = CqlSession.builder().build()) {
      createSchema(session);
      registerCoordinatesCodec(session);
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

  private static void registerCoordinatesCodec(CqlSession session) {
    // retrieve the codec registry
    MutableCodecRegistry codecRegistry =
        (MutableCodecRegistry) session.getContext().getCodecRegistry();
    // create the tuple metadata
    TupleType coordinatesType = DataTypes.tupleOf(DataTypes.INT, DataTypes.INT);
    // retrieve the driver built-in codec for the tuple "coordinates"
    TypeCodec<TupleValue> innerCodec = codecRegistry.codecFor(coordinatesType);
    // create a custom codec to map the "coordinates" tuple to the Coordinates class
    CoordinatesCodec coordinatesCodec = new CoordinatesCodec(innerCodec);
    // register the new codec
    codecRegistry.register(coordinatesCodec);
  }

  private static void insertData(CqlSession session) {
    // prepare the INSERT statement
    PreparedStatement prepared =
        session.prepare("INSERT INTO examples.tuples (k, c) VALUES (?, ?)");

    // bind the parameters in one pass
    Coordinates coordinates1 = new Coordinates(12, 34);
    BoundStatement boundStatement1 = prepared.bind(1, coordinates1);
    // execute the insertion
    session.execute(boundStatement1);

    // alternate method: bind the parameters one by one
    Coordinates coordinates2 = new Coordinates(56, 78);
    BoundStatement boundStatement2 =
        prepared.bind().setInt("k", 2).set("c", coordinates2, Coordinates.class);
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
      Coordinates coordinatesValue = row.get("c", Coordinates.class);
      assert coordinatesValue != null;

      // Display the contents of the Coordinates instance
      System.out.println("found coordinate: " + coordinatesValue);
    }
  }
}
