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
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Inserts and retrieves values using a few custom codecs.
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
 *   <li>creates a table "examples.videos". If it already exists, it will be reused;
 *   <li>inserts data in the table.
 * </ul>
 *
 * More examples of custom codecs can be found in the following examples:
 *
 * <ol>
 *   <li>Codecs for tuples and UDTs:
 *       <ul>
 *         <li>{@link TuplesSimple}
 *         <li>{@link TuplesMapped}
 *         <li>{@link UserDefinedTypesSimple}
 *         <li>{@link UserDefinedTypesMapped}
 *       </ul>
 *   <li>Json codecs:
 *       <ul>
 *         <li>{@link com.datastax.oss.driver.examples.json.jackson.JacksonJsonColumn}
 *         <li>{@link com.datastax.oss.driver.examples.json.jackson.JacksonJsonFunction}
 *         <li>{@link com.datastax.oss.driver.examples.json.jackson.JacksonJsonRow}
 *         <li>{@link com.datastax.oss.driver.examples.json.jsr.Jsr353JsonColumn}
 *         <li>{@link com.datastax.oss.driver.examples.json.jsr.Jsr353JsonFunction}
 *         <li>{@link com.datastax.oss.driver.examples.json.jsr.Jsr353JsonRow}
 *       </ul>
 *
 * @see <a
 *     href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/custom_codecs/">driver
 *     documentation on custom codecs</a>
 */
public class CustomCodecs {

  public static final GenericType<Optional<InetAddress>> OPTIONAL_OF_INET =
      GenericType.optionalOf(InetAddress.class);

  /** A dummy codec converting CQL ints into Java strings. */
  public static class CqlIntToStringCodec extends MappingCodec<Integer, String> {

    public CqlIntToStringCodec() {
      super(TypeCodecs.INT, GenericType.STRING);
    }

    @Nullable
    @Override
    protected String innerToOuter(@Nullable Integer value) {
      return value == null ? null : value.toString();
    }

    @Nullable
    @Override
    protected Integer outerToInner(@Nullable String value) {
      return value == null ? null : Integer.parseInt(value);
    }
  }

  public enum WeekDay {
    MONDAY,
    TUESDAY,
    WEDNESDAY,
    THURSDAY,
    FRIDAY,
    SATURDAY,
    SUNDAY
  }

  public static void main(String[] args) {
    CqlSessionBuilder builder = CqlSession.builder();
    builder = registerCodecs(builder);
    try (CqlSession session = builder.build()) {
      createSchema(session);
      insertData(session);
      retrieveData(session);
    }
  }

  private static CqlSessionBuilder registerCodecs(CqlSessionBuilder builder) {
    return builder.addTypeCodecs(
        ExtraTypeCodecs.BLOB_TO_ARRAY, // blob <-> byte[]
        ExtraTypeCodecs.ZONED_TIMESTAMP_PERSISTED, // tuple<timestamp,text> <-> ZonedDateTime
        ExtraTypeCodecs.listToArrayOf(TypeCodecs.TEXT), // list<text> <-> String[]
        ExtraTypeCodecs.enumNamesOf(WeekDay.class), // text <-> MyEnum
        ExtraTypeCodecs.optionalOf(TypeCodecs.INET), // uuid <-> Optional<InetAddress>
        new CqlIntToStringCodec() // custom codec, int <-> String
        );
  }

  private static void createSchema(CqlSession session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS examples "
            + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    session.execute(
        "CREATE TABLE IF NOT EXISTS examples.videos("
            + "pk int PRIMARY KEY, "
            + "contents blob, "
            + "uploaded tuple<timestamp,text>, "
            + "tags list<text>, "
            + "week_day text, "
            + "ip inet"
            + ")");
  }

  private static void insertData(CqlSession session) {
    // prepare the INSERT statement
    PreparedStatement prepared =
        session.prepare(
            "INSERT INTO examples.videos (pk, contents, uploaded, tags, week_day, ip) "
                + "VALUES (:pk, :contents, :uploaded, :tags, :week_day, :ip)");

    byte[] contents = new byte[] {1, 2, 3, 4};
    ZonedDateTime uploaded = ZonedDateTime.parse("2020-03-21T15:03:45.123+01:00[Europe/Paris]");
    String[] tags = new String[] {"comedy", "US"};
    WeekDay weekDay = WeekDay.SATURDAY;
    Optional<InetAddress> maybeIp = Optional.empty();

    // Create a BoundStatement and set values
    BoundStatement boundStatement =
        prepared
            .bind()
            .setString("pk", "1") // will use CqlIntToStringCodec
            .set("contents", contents, byte[].class) // will use TypeCodecs.BLOB_SIMPLE
            .set(
                "uploaded",
                uploaded,
                ZonedDateTime.class) // will use TypeCodecs.ZONED_TIMESTAMP_PERSISTED
            .set("tags", tags, String[].class) // will use  TypeCodecs.arrayOf(TypeCodecs.TEXT)
            .set(
                "week_day",
                weekDay,
                WeekDay.class) // will use TypeCodecs.enumNamesOf(WeekDay.class)
            .set(
                "ip", maybeIp, OPTIONAL_OF_INET); // will use TypeCodecs.optionalOf(TypeCodecs.INET)

    // execute the insertion
    session.execute(boundStatement);
  }

  private static void retrieveData(CqlSession session) {
    // Execute the SELECT query and retrieve the single row in the result set
    SimpleStatement statement =
        SimpleStatement.newInstance(
            "SELECT pk, contents, uploaded, tags, week_day, ip FROM examples.videos WHERE pk = ?",
            // Here, the primary key must be provided as an int, not as a String, because it is not
            // possible to use custom codecs in simple statements, only driver built-in codecs.
            // If this is an issue, use prepared statements.
            1);
    Row row = session.execute(statement).one();
    assert row != null;

    {
      // Retrieve values from row using custom codecs
      String pk = row.getString("pk"); // will use CqlIntToStringCodec
      byte[] contents = row.get("contents", byte[].class); // will use TypeCodecs.BLOB_SIMPLE
      ZonedDateTime uploaded =
          row.get("uploaded", ZonedDateTime.class); // will use TypeCodecs.ZONED_TIMESTAMP_PERSISTED
      String[] tags =
          row.get("tags", String[].class); // will use  TypeCodecs.arrayOf(TypeCodecs.TEXT)
      WeekDay weekDay =
          row.get("week_day", WeekDay.class); // will use TypeCodecs.enumNamesOf(WeekDay.class)
      Optional<InetAddress> maybeIp =
          row.get("ip", OPTIONAL_OF_INET); // will use TypeCodecs.optionalOf(TypeCodecs.INET)

      System.out.println("pk: " + pk);
      System.out.println("contents: " + Arrays.toString(contents));
      System.out.println("uploaded: " + uploaded);
      System.out.println("tags: " + Arrays.toString(tags));
      System.out.println("week day: " + weekDay);
      System.out.println("ip: " + maybeIp);
    }

    System.out.println("------------------");

    {
      // It is still possible to retrieve the same values from row using driver built-in codecs
      int pk = row.getInt("pk");
      ByteBuffer contents = row.getByteBuffer("contents");
      TupleValue uploaded = row.getTupleValue("uploaded");
      List<String> tags = row.getList("tags", String.class);
      String weekDay = row.getString("week_day");
      InetAddress ip = row.getInetAddress("ip");

      System.out.println("pk: " + pk);
      System.out.println("contents: " + ByteUtils.toHexString(contents));
      System.out.println(
          "uploaded: " + (uploaded == null ? null : uploaded.getFormattedContents()));
      System.out.println("tags: " + tags);
      System.out.println("week day: " + weekDay);
      System.out.println("ip: " + ip);
    }
  }
}
