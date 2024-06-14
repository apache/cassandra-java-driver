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
package com.datastax.oss.driver.core.type.codec.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.ccm.SchemaChangeSynchronizer;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.type.codec.IntCodec;
import com.datastax.oss.driver.internal.core.type.codec.UdtCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.OptionalCodec;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.assertj.core.util.Maps;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class CodecRegistryIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @Rule public TestName name = new TestName();

  @BeforeClass
  public static void createSchema() {
    SchemaChangeSynchronizer.withLock(
        () -> {
          // table with simple primary key, single cell.
          SESSION_RULE
              .session()
              .execute(
                  SimpleStatement.builder(
                          "CREATE TABLE IF NOT EXISTS test (k text primary key, v int)")
                      .setExecutionProfile(SESSION_RULE.slowProfile())
                      .build());
          // table with map value
          SESSION_RULE
              .session()
              .execute(
                  SimpleStatement.builder(
                          "CREATE TABLE IF NOT EXISTS test2 (k0 text, k1 int, v map<int,text>, primary key (k0, k1))")
                      .setExecutionProfile(SESSION_RULE.slowProfile())
                      .build());
          // table with UDT
          SESSION_RULE
              .session()
              .execute(
                  SimpleStatement.builder("CREATE TYPE IF NOT EXISTS coordinates (x int, y int)")
                      .setExecutionProfile(SESSION_RULE.slowProfile())
                      .build());
          SESSION_RULE
              .session()
              .execute(
                  SimpleStatement.builder(
                          "CREATE TABLE IF NOT EXISTS test3 (k0 text, k1 int, v map<text,frozen<coordinates>>, primary key (k0, k1))")
                      .setExecutionProfile(SESSION_RULE.slowProfile())
                      .build());
        });
  }

  // A simple codec that allows float values to be used for cassandra int column type.
  private static class FloatCIntCodec implements TypeCodec<Float> {

    private static final IntCodec intCodec = new IntCodec();

    @NonNull
    @Override
    public GenericType<Float> getJavaType() {
      return GenericType.of(Float.class);
    }

    @NonNull
    @Override
    public DataType getCqlType() {
      return DataTypes.INT;
    }

    @Override
    public ByteBuffer encode(Float value, @NonNull ProtocolVersion protocolVersion) {
      return intCodec.encode(value.intValue(), protocolVersion);
    }

    @Override
    public Float decode(ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
      return intCodec.decode(bytes, protocolVersion).floatValue();
    }

    @NonNull
    @Override
    public String format(Float value) {
      return intCodec.format(value.intValue());
    }

    @Override
    public Float parse(String value) {
      return intCodec.parse(value).floatValue();
    }
  }

  @Test
  public void should_throw_exception_if_no_codec_registered_for_type_set() {
    PreparedStatement prepared =
        SESSION_RULE.session().prepare("INSERT INTO test (k, v) values (?, ?)");

    // float value for int column should not work since no applicable codec.
    Throwable t =
        catchThrowable(
            () ->
                prepared
                    .boundStatementBuilder()
                    .setString(0, name.getMethodName())
                    .setFloat(1, 3.14f)
                    .build());

    assertThat(t).isInstanceOf(CodecNotFoundException.class);
  }

  @Test
  public void should_throw_exception_if_no_codec_registered_for_type_get() {
    PreparedStatement prepared =
        SESSION_RULE.session().prepare("INSERT INTO test (k, v) values (?, ?)");

    BoundStatement insert =
        prepared.boundStatementBuilder().setString(0, name.getMethodName()).setInt(1, 2).build();
    SESSION_RULE.session().execute(insert);

    ResultSet result =
        SESSION_RULE
            .session()
            .execute(
                SimpleStatement.builder("SELECT v from test where k = ?")
                    .addPositionalValue(name.getMethodName())
                    .build());

    List<Row> rows = result.all();
    assertThat(rows).hasSize(1);

    // should not be able to access int column as float as no codec is registered to handle that.
    Row row = rows.iterator().next();

    Throwable t = catchThrowable(() -> assertThat(row.getFloat("v")).isEqualTo(3.0f));

    assertThat(t).isInstanceOf(CodecNotFoundException.class);
  }

  @Test
  public void should_be_able_to_register_and_use_custom_codec() {
    // create a cluster with a registered codec from Float <-> cql int.
    try (CqlSession session =
        (CqlSession)
            SessionUtils.baseBuilder()
                .addTypeCodecs(new FloatCIntCodec())
                .addContactEndPoints(CCM_RULE.getContactPoints())
                .withKeyspace(SESSION_RULE.keyspace())
                .build()) {
      PreparedStatement prepared = session.prepare("INSERT INTO test (k, v) values (?, ?)");

      // float value for int column should work.
      BoundStatement insert =
          prepared
              .boundStatementBuilder()
              .setString(0, name.getMethodName())
              .setFloat(1, 3.14f)
              .build();
      session.execute(insert);

      ResultSet result =
          session.execute(
              SimpleStatement.builder("SELECT v from test where k = ?")
                  .addPositionalValue(name.getMethodName())
                  .build());

      List<Row> rows = result.all();
      assertThat(rows).hasSize(1);

      // should be able to retrieve value back as float, some precision is lost due to going from
      // int -> float.
      Row row = rows.iterator().next();
      assertThat(row.getFloat("v")).isEqualTo(3.0f);
      assertThat(row.getFloat(0)).isEqualTo(3.0f);
    }
  }

  @Test
  public void should_register_custom_codec_at_runtime() {
    // Still create a separate session because we don't want to interfere with other tests
    try (CqlSession session = SessionUtils.newSession(CCM_RULE, SESSION_RULE.keyspace())) {

      MutableCodecRegistry registry =
          (MutableCodecRegistry) session.getContext().getCodecRegistry();
      registry.register(new FloatCIntCodec());

      PreparedStatement prepared = session.prepare("INSERT INTO test (k, v) values (?, ?)");

      // float value for int column should work.
      BoundStatement insert =
          prepared
              .boundStatementBuilder()
              .setString(0, name.getMethodName())
              .setFloat(1, 3.14f)
              .build();
      session.execute(insert);

      ResultSet result =
          session.execute(
              SimpleStatement.builder("SELECT v from test where k = ?")
                  .addPositionalValue(name.getMethodName())
                  .build());

      List<Row> rows = result.all();
      assertThat(rows).hasSize(1);

      // should be able to retrieve value back as float, some precision is lost due to going from
      // int -> float.
      Row row = rows.iterator().next();
      assertThat(row.getFloat("v")).isEqualTo(3.0f);
      assertThat(row.getFloat(0)).isEqualTo(3.0f);
    }
  }

  @Test
  public void should_be_able_to_register_and_use_custom_codec_with_generic_type() {
    // create a cluster with registered codecs using OptionalCodec
    OptionalCodec<Map<Integer, String>> optionalMapCodec =
        new OptionalCodec<>(TypeCodecs.mapOf(TypeCodecs.INT, TypeCodecs.TEXT));
    TypeCodec<Map<Integer, Optional<String>>> mapWithOptionalValueCodec =
        TypeCodecs.mapOf(TypeCodecs.INT, new OptionalCodec<>(TypeCodecs.TEXT));

    try (CqlSession session =
        (CqlSession)
            SessionUtils.baseBuilder()
                .addTypeCodecs(optionalMapCodec, mapWithOptionalValueCodec)
                .addContactEndPoints(CCM_RULE.getContactPoints())
                .withKeyspace(SESSION_RULE.keyspace())
                .build()) {
      PreparedStatement prepared =
          session.prepare("INSERT INTO test2 (k0, k1, v) values (?, ?, ?)");

      // optional map should work.
      Map<Integer, String> v0 = Maps.newHashMap(0, "value");
      Optional<Map<Integer, String>> v0Opt = Optional.of(v0);
      BoundStatement insert =
          prepared
              .boundStatementBuilder()
              .setString(0, name.getMethodName())
              .setInt(1, 0)
              .set(
                  2,
                  v0Opt,
                  optionalMapCodec
                      .getJavaType()) // use java type so has to be looked up in registry.
              .build();
      session.execute(insert);

      // optional absent map should work.
      Optional<Map<Integer, String>> absentMap = Optional.empty();
      insert =
          prepared
              .boundStatementBuilder()
              .setString(0, name.getMethodName())
              .setInt(1, 1)
              .set(2, absentMap, optionalMapCodec.getJavaType())
              .build();
      session.execute(insert);

      // map with optional value should work - note that you can't have null values in collections,
      // so this is not technically practical but want to validate that custom codec resolution
      // works
      // when it's composed in a collection codec.
      Map<Integer, Optional<String>> v2Map = Maps.newHashMap(1, Optional.of("hello"));
      insert =
          prepared
              .boundStatementBuilder()
              .setString(0, name.getMethodName())
              .setInt(1, 2)
              .set(2, v2Map, mapWithOptionalValueCodec.getJavaType())
              .build();
      session.execute(insert);

      ResultSet result =
          session.execute(
              SimpleStatement.builder("SELECT v from test2 where k0 = ?")
                  .addPositionalValues(name.getMethodName())
                  .build());

      List<Row> rows = result.all();
      assertThat(rows).hasSize(3);

      Iterator<Row> iterator = rows.iterator();
      // row (at key 0) should have v0
      Row row = iterator.next();
      // should be able to retrieve value back as an optional map.
      assertThat(row.get(0, optionalMapCodec.getJavaType())).isEqualTo(v0Opt);
      // should be able to retrieve value back as map.
      assertThat(row.getMap(0, Integer.class, String.class)).isEqualTo(v0);

      // next row (at key 1) should be absent (null value).
      row = iterator.next();
      // value should be null.
      assertThat(row.isNull(0)).isTrue();
      // getting with codec should return Optional.empty()
      assertThat(row.get(0, optionalMapCodec.getJavaType())).isEqualTo(absentMap);
      // getting with map should return an empty map.
      assertThat(row.getMap(0, Integer.class, String.class)).isEmpty();

      // next row (at key 2) should have v2
      row = iterator.next();
      // getting with codec should return with the correct type.
      assertThat(row.get(0, mapWithOptionalValueCodec.getJavaType())).isEqualTo(v2Map);
      // getting with map should return a map without optional value.
      assertThat(row.getMap(0, Integer.class, String.class)).isEqualTo(Maps.newHashMap(1, "hello"));
    }
  }

  @Test
  public void should_be_able_to_handle_empty_collections() {
    try (CqlSession session =
        (CqlSession)
            SessionUtils.<CqlSession>baseBuilder()
                .addContactEndPoints(CCM_RULE.getContactPoints())
                .withKeyspace(SESSION_RULE.keyspace())
                .build()) {

      // Using prepared statements (CQL type is known)
      PreparedStatement prepared =
          session.prepare("INSERT INTO test2 (k0, k1, v) values (?, ?, ?)");

      BoundStatement insert =
          prepared
              .boundStatementBuilder()
              .setString(0, name.getMethodName())
              .setInt(1, 0)
              .setMap(2, new HashMap<>(), Integer.class, String.class)
              .build();
      session.execute(insert);

      // Using simple statements (CQL type is unknown)
      session.execute(
          SimpleStatement.newInstance(
              "INSERT INTO test2 (k0, k1, v) values (?, ?, ?)",
              name.getMethodName(),
              1,
              new HashMap<>()));

      ResultSet result =
          session.execute(
              SimpleStatement.builder("SELECT v from test2 where k0 = ?")
                  .addPositionalValues(name.getMethodName())
                  .build());

      List<Row> rows = result.all();
      assertThat(rows).hasSize(2);

      Row row1 = rows.get(0);
      assertThat(row1.isNull(0)).isTrue();
      assertThat(row1.getMap(0, Integer.class, String.class)).isEmpty();

      Row row2 = rows.get(1);
      assertThat(row2.isNull(0)).isTrue();
      assertThat(row2.getMap(0, Integer.class, String.class)).isEmpty();
    }
  }

  private static final class Coordinates {

    public final int x;
    public final int y;

    public Coordinates(int x, int y) {
      this.x = x;
      this.y = y;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Coordinates that = (Coordinates) o;
      return this.x == that.x && this.y == that.y;
    }

    @Override
    public int hashCode() {
      return Objects.hash(x, y);
    }
  }

  private static class CoordinatesCodec extends MappingCodec<UdtValue, Coordinates> {

    public CoordinatesCodec(@NonNull TypeCodec<UdtValue> innerCodec) {
      super(innerCodec, GenericType.of(Coordinates.class));
    }

    @NonNull
    @Override
    public UserDefinedType getCqlType() {
      return (UserDefinedType) super.getCqlType();
    }

    @Nullable
    @Override
    protected Coordinates innerToOuter(@Nullable UdtValue value) {
      return value == null ? null : new Coordinates(value.getInt("x"), value.getInt("y"));
    }

    @Nullable
    @Override
    protected UdtValue outerToInner(@Nullable Coordinates value) {
      return value == null
          ? null
          : getCqlType().newValue().setInt("x", value.x).setInt("y", value.y);
    }
  }

  @Test
  public void should_register_and_use_custom_codec_for_user_defined_type() {

    Map<String, Coordinates> coordinatesMap = ImmutableMap.of("home", new Coordinates(12, 34));
    GenericType<Map<String, Coordinates>> coordinatesMapType =
        GenericType.mapOf(String.class, Coordinates.class);

    // Still create a separate session because we don't want to interfere with other tests
    try (CqlSession session = SessionUtils.newSession(CCM_RULE, SESSION_RULE.keyspace())) {

      // register the mapping codec for UDT coordinates
      UserDefinedType coordinatesUdt =
          session
              .getMetadata()
              .getKeyspace(SESSION_RULE.keyspace())
              .flatMap(ks -> ks.getUserDefinedType("coordinates"))
              .orElseThrow(IllegalStateException::new);
      MutableCodecRegistry codecRegistry =
          (MutableCodecRegistry) session.getContext().getCodecRegistry();

      // Retrieve the inner codec
      TypeCodec<UdtValue> innerCodec = codecRegistry.codecFor(coordinatesUdt);
      assertThat(innerCodec).isInstanceOf(UdtCodec.class);

      // Create the "outer" codec and register it
      CoordinatesCodec coordinatesCodec = new CoordinatesCodec(innerCodec);
      codecRegistry.register(coordinatesCodec);

      // Test that the codec will be used to create on-the-fly codecs
      assertThat(codecRegistry.codecFor(Coordinates.class)).isSameAs(coordinatesCodec);
      assertThat(codecRegistry.codecFor(coordinatesMapType).accepts(coordinatesMap)).isTrue();

      // test insertion
      PreparedStatement prepared =
          session.prepare("INSERT INTO test3 (k0, k1, v) values (?, ?, ?)");
      BoundStatement insert =
          prepared
              .boundStatementBuilder()
              .setString(0, name.getMethodName())
              .setInt(1, 0)
              .set(
                  2,
                  coordinatesMap,
                  coordinatesMapType) // use java type so has to be looked up in registry.
              .build();
      session.execute(insert);

      // test retrieval
      ResultSet result =
          session.execute(
              SimpleStatement.builder("SELECT v from test3 where k0 = ? AND k1 = ?")
                  .addPositionalValues(name.getMethodName(), 0)
                  .build());
      List<Row> rows = result.all();
      assertThat(rows).hasSize(1);
      Row row = rows.get(0);
      assertThat(row.get(0, coordinatesMapType)).isEqualTo(coordinatesMap);
      assertThat(row.getMap(0, String.class, Coordinates.class)).isEqualTo(coordinatesMap);
    }
  }
}
