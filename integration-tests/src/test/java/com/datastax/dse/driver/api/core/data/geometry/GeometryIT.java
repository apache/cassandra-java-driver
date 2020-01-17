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
package com.datastax.dse.driver.api.core.data.geometry;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.assertj.core.util.Preconditions;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ParallelizableTests.class})
public abstract class GeometryIT<T extends Geometry> {

  private final Class<T> genericType;
  private final T baseSample;
  private final List<T> sampleData;
  private final SessionRule<CqlSession> sessionRule;

  @SuppressWarnings("unchecked")
  GeometryIT(List<T> sampleData, Class<T> genericType, SessionRule<CqlSession> sessionRule) {
    Preconditions.checkArgument(
        sampleData.size() >= 3, "Must be at least 3 samples, was given " + sampleData.size());
    this.baseSample = sampleData.get(0);
    this.genericType = genericType;
    this.sampleData = sampleData;
    this.sessionRule = sessionRule;
  }

  static void onTestContextInitialized(String cqlTypeName, SessionRule<CqlSession> sessionRule) {
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder(String.format("CREATE TYPE udt1 (g '%s')", cqlTypeName))
                .setExecutionProfile(sessionRule.slowProfile())
                .build());
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder(
                    String.format(
                        "CREATE TABLE tbl (k uuid PRIMARY KEY, g '%s', l list<'%s'>, s set<'%s'>, m0 map<'%s',int>, m1 map<int,'%s'>, t tuple<'%s','%s','%s'>, u frozen<udt1>)",
                        cqlTypeName,
                        cqlTypeName,
                        cqlTypeName,
                        cqlTypeName,
                        cqlTypeName,
                        cqlTypeName,
                        cqlTypeName,
                        cqlTypeName))
                .setExecutionProfile(sessionRule.slowProfile())
                .build());
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder(
                    String.format("CREATE TABLE tblpk (k '%s' primary key, v int)", cqlTypeName))
                .setExecutionProfile(sessionRule.slowProfile())
                .build());
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder(
                    String.format(
                        "CREATE TABLE tblclustering (k0 int, k1 '%s', v int, primary key (k0, k1))",
                        cqlTypeName))
                .setExecutionProfile(sessionRule.slowProfile())
                .build());
  }

  private <V> void validate(UUID key, String columnName, V expected, GenericType<V> type) {
    ResultSet result =
        sessionRule
            .session()
            .execute(
                SimpleStatement.builder(
                        String.format("SELECT k,%s FROM tbl where k =? ", columnName))
                    .addPositionalValue(key)
                    .build());
    Row row = result.iterator().next();
    assertThat(row.getUuid("k")).isEqualTo(key);
    assertThat(row.get(columnName, type)).isEqualTo(expected);
    assertThat(row.get(1, type)).isEqualTo(expected);
  }

  private void validate(UUID key, T expected) {
    validate(key, "g", expected, GenericType.of(genericType));
  }

  /**
   * Validates that a given geometry value can be inserted into a column using codec.format() and
   * verifies that it is stored correctly by retrieving it and ensuring it matches.
   */
  @Test
  public void should_insert_using_format() {
    for (T expected : sampleData) {

      String val = null;
      if (expected != null) {
        TypeCodec<T> codec =
            sessionRule.session().getContext().getCodecRegistry().codecFor(expected);
        val = codec.format(expected);
      }
      UUID key = Uuids.random();
      sessionRule
          .session()
          .execute(String.format("INSERT INTO tbl (k, g) VALUES (%s, %s)", key, val));
      validate(key, expected);
    }
  }

  /**
   * Validates that a given geometry value can be inserted into a column by providing it as a simple
   * statement parameter and verifies that it is stored correctly by retrieving it and ensuring it
   * matches.
   */
  @Test
  public void should_insert_using_simple_statement_with_parameters() {
    for (T expected : sampleData) {
      UUID key = Uuids.random();
      sessionRule
          .session()
          .execute(
              SimpleStatement.builder("INSERT INTO tbl (k, g) VALUES (?, ?)")
                  .addPositionalValues(key, expected)
                  .build());
      validate(key, expected);
    }
  }
  /**
   * Validates that a given geometry value can be inserted into a column by providing it as a bound
   * parameter in a BoundStatement and verifies that it is stored correctly by retrieving it and
   * ensuring it matches.
   */
  @Test
  public void should_insert_using_prepared_statement_with_parameters() {
    for (T expected : sampleData) {
      UUID key = Uuids.random();
      PreparedStatement prepared =
          sessionRule.session().prepare("INSERT INTO tbl (k, g) values (?, ?)");
      BoundStatement bs =
          prepared.boundStatementBuilder().setUuid(0, key).set(1, expected, genericType).build();
      sessionRule.session().execute(bs);
      validate(key, expected);
    }
  }
  /**
   * Validates that geometry values can be inserted as a list and verifies that the list is stored
   * correctly by retrieving it and ensuring it matches.
   */
  @Test
  public void should_insert_as_list() {
    UUID key = Uuids.random();
    PreparedStatement prepared =
        sessionRule.session().prepare("INSERT INTO tbl (k, l) values (?, ?)");

    BoundStatement bs =
        prepared
            .boundStatementBuilder()
            .setUuid(0, key)
            .setList(1, sampleData, genericType)
            .build();
    sessionRule.session().execute(bs);
    validate(key, "l", sampleData, GenericType.listOf(genericType));
  }
  /**
   * Validates that geometry values can be inserted as a set and verifies that the set is stored
   * correctly by retrieving it and ensuring it matches.
   */
  @Test
  public void should_insert_as_set() {
    UUID key = Uuids.random();
    Set<T> asSet = Sets.newHashSet(sampleData);

    PreparedStatement prepared =
        sessionRule.session().prepare("INSERT INTO tbl (k, s) values (?, ?)");
    BoundStatement bs =
        prepared.boundStatementBuilder().setUuid(0, key).setSet(1, asSet, genericType).build();

    sessionRule.session().execute(bs);
    validate(key, "s", asSet, GenericType.setOf(genericType));
  }

  /**
   * Validates that geometry values can be inserted into a map as keys and verifies that the map is
   * stored correctly by retrieving it and ensuring it matches.
   */
  @Test
  public void should_insert_as_map_keys() {
    UUID key = Uuids.random();
    ImmutableMap.Builder<T, Integer> builder = ImmutableMap.builder();
    int count = 0;
    for (T val : sampleData) {
      builder = builder.put(val, count++);
    }
    Map<T, Integer> asMapKeys = builder.build();

    PreparedStatement prepared =
        sessionRule.session().prepare("INSERT INTO tbl (k, m0) values (?, ?)");
    BoundStatement bs =
        prepared
            .boundStatementBuilder()
            .setUuid(0, key)
            .setMap(1, asMapKeys, genericType, Integer.class)
            .build();
    sessionRule.session().execute(bs);
    validate(key, "m0", asMapKeys, GenericType.mapOf(genericType, Integer.class));
  }

  /**
   * Validates that geometry values can be inserted into a map as values and verifies that the map
   * is stored correctly by retrieving it and ensuring it matches.
   */
  @Test
  public void should_insert_as_map_values() {
    UUID key = Uuids.random();
    ImmutableMap.Builder<Integer, T> builder = ImmutableMap.builder();
    int count = 0;
    for (T val : sampleData) {
      builder = builder.put(count++, val);
    }
    Map<Integer, T> asMapValues = builder.build();
    PreparedStatement prepared =
        sessionRule.session().prepare("INSERT INTO tbl (k, m1) values (?, ?)");
    BoundStatement bs =
        prepared
            .boundStatementBuilder()
            .setUuid(0, key)
            .setMap(1, asMapValues, Integer.class, genericType)
            .build();
    sessionRule.session().execute(bs);
    validate(key, "m1", asMapValues, GenericType.mapOf(Integer.class, genericType));
  }

  /**
   * Validates that geometry values can be inserted as a tuple and verifies that the tuple is stored
   * correctly by retrieving it and ensuring it matches.
   */
  @Test
  @Ignore
  public void should_insert_as_tuple() {
    UUID key = Uuids.random();

    PreparedStatement prepared =
        sessionRule.session().prepare("INSERT INTO tbl (k, t) values (?, ?)");
    TupleType tupleType = (TupleType) prepared.getVariableDefinitions().get(1).getType();
    TupleValue tuple = tupleType.newValue();
    tuple = tuple.set(0, sampleData.get(0), genericType);
    tuple = tuple.set(1, sampleData.get(1), genericType);
    tuple = tuple.set(2, sampleData.get(2), genericType);
    BoundStatement bs =
        prepared.boundStatementBuilder().setUuid(0, key).setTupleValue(1, tuple).build();
    sessionRule.session().execute(bs);
    ResultSet rs =
        sessionRule
            .session()
            .execute(
                SimpleStatement.builder("SELECT k,t FROM tbl where k=?")
                    .addPositionalValues(key)
                    .build());
    Row row = rs.iterator().next();
    assertThat(row.getUuid("k")).isEqualTo(key);
    assertThat(row.getTupleValue("t")).isEqualTo(tuple);
    assertThat(row.getTupleValue(1)).isEqualTo(tuple);
  }
  /**
   * Validates that a geometry value can be inserted as a field in a UDT and verifies that the UDT
   * is stored correctly by retrieving it and ensuring it matches.
   */
  @Test
  @Ignore
  public void should_insert_as_field_in_udt() {
    UUID key = Uuids.random();
    UserDefinedType udtType =
        sessionRule
            .session()
            .getMetadata()
            .getKeyspace(sessionRule.session().getKeyspace().orElseThrow(AssertionError::new))
            .flatMap(ks -> ks.getUserDefinedType(CqlIdentifier.fromInternal("udt1")))
            .orElseThrow(AssertionError::new);
    assertThat(udtType).isNotNull();
    UdtValue value = udtType.newValue();
    value = value.set("g", sampleData.get(0), genericType);

    PreparedStatement prepared =
        sessionRule.session().prepare("INSERT INTO tbl (k, u) values (?, ?)");
    BoundStatement bs =
        prepared.boundStatementBuilder().setUuid(0, key).setUdtValue(1, value).build();
    sessionRule.session().execute(bs);

    ResultSet rs =
        sessionRule
            .session()
            .execute(
                SimpleStatement.builder("SELECT k,u FROM tbl where k=?")
                    .addPositionalValues(key)
                    .build());
    Row row = rs.iterator().next();
    assertThat(row.getUuid("k")).isEqualTo(key);
    assertThat(row.getUdtValue("u")).isEqualTo(value);
    assertThat(row.getUdtValue(1)).isEqualTo(value);
  }

  /**
   * Validates that a geometry value can be inserted into a column that is the partition key and
   * then validates that it can be queried back by partition key.
   */
  @Test
  public void should_accept_as_partition_key() {
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder("INSERT INTO tblpk (k, v) VALUES (?,?)")
                .addPositionalValues(baseSample, 1)
                .build());
    ResultSet results = sessionRule.session().execute("SELECT k,v FROM tblpk");
    Row row = results.one();
    T key = row.get("k", genericType);
    assertThat(key).isEqualTo(baseSample);
  }

  /**
   * Validates that geometry values can be inserted into a column that is a clustering key in rows
   * sharing a partition key and then validates that the rows can be retrieved by partition key.
   *
   * @test_category dse:geospatial
   */
  @Test
  public void should_accept_as_clustering_key() {
    PreparedStatement insert =
        sessionRule.session().prepare("INSERT INTO tblclustering (k0, k1, v) values (?,?,?)");
    BatchStatementBuilder batchbuilder = BatchStatement.builder(DefaultBatchType.UNLOGGED);

    int count = 0;
    for (T value : sampleData) {
      BoundStatement bound =
          insert
              .boundStatementBuilder()
              .setInt(0, 0)
              .set(1, value, genericType)
              .setInt(2, count++)
              .build();
      batchbuilder.addStatement(bound);
    }
    sessionRule.session().execute(batchbuilder.build());

    ResultSet result =
        sessionRule
            .session()
            .execute(
                SimpleStatement.builder("SELECT * from tblclustering where k0=?")
                    .addPositionalValue(0)
                    .build());

    // The order of rows returned is not significant for geospatial types since it is stored in
    // lexicographic byte order (8 bytes at a time). Thus we pull them all sort and extract and
    // ensure all values were returned.
    List<Row> rows = result.all();

    assertThat(rows)
        .extracting(row -> row.get("k1", genericType))
        .containsOnlyElementsOf(sampleData)
        .hasSameSizeAs(sampleData);
  }
}
