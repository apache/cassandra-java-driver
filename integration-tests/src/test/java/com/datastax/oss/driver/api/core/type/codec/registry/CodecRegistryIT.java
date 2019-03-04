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
package com.datastax.oss.driver.api.core.type.codec.registry;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.core.type.reflect.GenericTypeParameter;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.type.codec.IntCodec;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import org.assertj.core.util.Maps;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class CodecRegistryIT {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  @Rule public TestName name = new TestName();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void createSchema() {
    // table with simple primary key, single cell.
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder("CREATE TABLE IF NOT EXISTS test (k text primary key, v int)")
                .setExecutionProfile(sessionRule.slowProfile())
                .build());
    // table with map value
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder(
                    "CREATE TABLE IF NOT EXISTS test2 (k0 text, k1 int, v map<int,text>, primary key (k0, k1))")
                .setExecutionProfile(sessionRule.slowProfile())
                .build());
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
        sessionRule.session().prepare("INSERT INTO test (k, v) values (?, ?)");

    thrown.expect(CodecNotFoundException.class);

    // float value for int column should not work since no applicable codec.
    prepared.boundStatementBuilder().setString(0, name.getMethodName()).setFloat(1, 3.14f).build();
  }

  @Test
  public void should_throw_exception_if_no_codec_registered_for_type_get() {
    PreparedStatement prepared =
        sessionRule.session().prepare("INSERT INTO test (k, v) values (?, ?)");

    BoundStatement insert =
        prepared.boundStatementBuilder().setString(0, name.getMethodName()).setInt(1, 2).build();
    sessionRule.session().execute(insert);

    ResultSet result =
        sessionRule
            .session()
            .execute(
                SimpleStatement.builder("SELECT v from test where k = ?")
                    .addPositionalValue(name.getMethodName())
                    .build());

    List<Row> rows = result.all();
    assertThat(rows).hasSize(1);

    // should not be able to access int column as float as no codec is registered to handle that.
    Row row = rows.iterator().next();

    thrown.expect(CodecNotFoundException.class);

    assertThat(row.getFloat("v")).isEqualTo(3.0f);
  }

  @Test
  public void should_be_able_to_register_and_use_custom_codec() {
    // create a cluster with a registered codec from Float <-> cql int.
    try (CqlSession session =
        (CqlSession)
            SessionUtils.baseBuilder()
                .addTypeCodecs(new FloatCIntCodec())
                .addContactEndPoints(ccm.getContactPoints())
                .withKeyspace(sessionRule.keyspace())
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

  // TODO: consider moving this into source as it could be generally useful.
  private abstract static class MappingCodec<O, I> implements TypeCodec<O> {

    private final GenericType<O> javaType;
    private final TypeCodec<I> innerCodec;

    MappingCodec(TypeCodec<I> innerCodec, GenericType<O> javaType) {
      this.innerCodec = innerCodec;
      this.javaType = javaType;
    }

    @NonNull
    @Override
    public GenericType<O> getJavaType() {
      return javaType;
    }

    @NonNull
    @Override
    public DataType getCqlType() {
      return innerCodec.getCqlType();
    }

    @Override
    public ByteBuffer encode(O value, @NonNull ProtocolVersion protocolVersion) {
      return innerCodec.encode(encode(value), protocolVersion);
    }

    @Override
    public O decode(ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
      return decode(innerCodec.decode(bytes, protocolVersion));
    }

    @NonNull
    @Override
    public String format(O value) {
      return value == null ? null : innerCodec.format(encode(value));
    }

    @Override
    public O parse(String value) {
      return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
          ? null
          : decode(innerCodec.parse(value));
    }

    protected abstract O decode(I value);

    protected abstract I encode(O value);
  }

  private static class OptionalCodec<T> extends MappingCodec<Optional<T>, T> {

    // in cassandra, empty collections are considered null and vise versa.
    Predicate<T> isAbsent =
        (i) ->
            i == null
                || ((i instanceof Collection && ((Collection) i).isEmpty()))
                || ((i instanceof Map) && ((Map) i).isEmpty());

    OptionalCodec(TypeCodec<T> innerCodec) {
      super(
          innerCodec,
          new GenericType<Optional<T>>() {}.where(
              new GenericTypeParameter<T>() {}, innerCodec.getJavaType()));
    }

    @Override
    protected Optional<T> decode(T value) {
      return isAbsent.test(value) ? Optional.empty() : Optional.of(value);
    }

    @Override
    protected T encode(Optional<T> value) {
      return value.orElse(null);
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
                .addContactEndPoints(ccm.getContactPoints())
                .withKeyspace(sessionRule.keyspace())
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
}
