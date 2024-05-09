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
package com.datastax.oss.driver.core.type.codec;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class ExtraTypeCodecsIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private enum TableField {
    cql_text("text_value", "text"),
    cql_int("integer_value", "int"),
    cql_vector("vector_value", "vector<float, 3>"),
    cql_list_of_text("list_of_text_value", "list<text>"),
    cql_timestamp("timestamp_value", "timestamp"),
    cql_boolean("boolean_value", "boolean"),
    ;

    final String name;
    final String ty;

    TableField(String name, String ty) {
      this.name = name;
      this.ty = ty;
    }

    private String definition() {
      return String.format("%s %s", name, ty);
    }
  }

  @BeforeClass
  public static void setupSchema() {
    List<String> fieldDefinitions = new ArrayList<>();
    fieldDefinitions.add("key uuid PRIMARY KEY");
    Stream.of(TableField.values())
        .forEach(
            tf -> {
              // TODO: Move this check to BackendRequirementRule once JAVA-3069 is resolved.
              if (tf == TableField.cql_vector
                  && CCM_RULE.getCassandraVersion().compareTo(Version.parse("5.0")) < 0) {
                // don't add vector type before cassandra version 5.0
                return;
              }
              fieldDefinitions.add(tf.definition());
            });
    SESSION_RULE
        .session()
        .execute(
            SimpleStatement.builder(
                    String.format(
                        "CREATE TABLE IF NOT EXISTS extra_type_codecs_it (%s)",
                        String.join(", ", fieldDefinitions)))
                .setExecutionProfile(SESSION_RULE.slowProfile())
                .build());
  }

  private <T> void insertAndRead(TableField field, T value, TypeCodec<T> codec) {
    CqlSession session = SESSION_RULE.session();
    // write value under new key using provided codec
    UUID key = UUID.randomUUID();

    PreparedStatement preparedInsert =
        session.prepare(
            SimpleStatement.builder(
                    String.format(
                        "INSERT INTO extra_type_codecs_it (key, %s) VALUES (?, ?)", field.name))
                .build());
    BoundStatement boundInsert =
        preparedInsert
            .boundStatementBuilder()
            .setUuid("key", key)
            .set(field.name, value, codec)
            .build();
    session.execute(boundInsert);

    // read value using provided codec and assert result
    PreparedStatement preparedSelect =
        session.prepare(
            SimpleStatement.builder(
                    String.format("SELECT %s FROM extra_type_codecs_it WHERE key = ?", field.name))
                .build());
    BoundStatement boundSelect = preparedSelect.boundStatementBuilder().setUuid("key", key).build();
    assertThat(session.execute(boundSelect).one().get(field.name, codec)).isEqualTo(value);
  }

  @Test
  public void enum_names_of() {
    insertAndRead(
        TableField.cql_text, TestEnum.value1, ExtraTypeCodecs.enumNamesOf(TestEnum.class));
  }

  @Test
  public void enum_ordinals_of() {
    insertAndRead(
        TableField.cql_int, TestEnum.value1, ExtraTypeCodecs.enumOrdinalsOf(TestEnum.class));
  }

  // Also requires -Dccm.branch=vsearch and the ability to build that branch locally
  @BackendRequirement(type = BackendType.CASSANDRA, minInclusive = "5.0.0")
  @Test
  public void float_to_vector_array() {
    // @BackRequirement on test methods that use @ClassRule to configure CcmRule require @Rule
    // BackendRequirementRule included with fix JAVA-3069. Until then we will ignore this test with
    // an assume.
    Assume.assumeTrue(
        "Requires Cassandra 5.0 or greater",
        CCM_RULE.getCassandraVersion().compareTo(Version.parse("5.0")) >= 0);
    insertAndRead(
        TableField.cql_vector,
        new float[] {1.1f, 0f, Float.NaN},
        ExtraTypeCodecs.floatVectorToArray(3));
  }

  @Test
  public void json_java_class() {
    insertAndRead(
        TableField.cql_text,
        new TestJsonAnnotatedPojo("example", Arrays.asList(1, 2, 3)),
        ExtraTypeCodecs.json(TestJsonAnnotatedPojo.class));
  }

  @Test
  public void json_java_class_and_object_mapper() {
    insertAndRead(
        TableField.cql_text,
        TestPojo.create(1, "abc", "def"),
        ExtraTypeCodecs.json(TestPojo.class, new ObjectMapper()));
  }

  @Test
  public void list_to_array_of() {
    insertAndRead(
        TableField.cql_list_of_text,
        new String[] {"hello", "kitty"},
        ExtraTypeCodecs.listToArrayOf(TypeCodecs.TEXT));
  }

  @Test
  public void local_timestamp_at() {
    ZoneId systemZoneId = ZoneId.systemDefault();
    insertAndRead(
        TableField.cql_timestamp,
        LocalDateTime.now(systemZoneId).truncatedTo(ChronoUnit.MILLIS),
        ExtraTypeCodecs.localTimestampAt(systemZoneId));
  }

  @Test
  public void optional_of() {
    insertAndRead(
        TableField.cql_boolean, Optional.empty(), ExtraTypeCodecs.optionalOf(TypeCodecs.BOOLEAN));
    insertAndRead(
        TableField.cql_boolean, Optional.of(true), ExtraTypeCodecs.optionalOf(TypeCodecs.BOOLEAN));
  }

  @Test
  public void timestamp_at() {
    ZoneId systemZoneId = ZoneId.systemDefault();
    insertAndRead(
        TableField.cql_timestamp,
        Instant.now().truncatedTo(ChronoUnit.MILLIS),
        ExtraTypeCodecs.timestampAt(systemZoneId));
  }

  @Test
  public void timestamp_millis_at() {
    ZoneId systemZoneId = ZoneId.systemDefault();
    insertAndRead(
        TableField.cql_timestamp,
        Instant.now().toEpochMilli(),
        ExtraTypeCodecs.timestampMillisAt(systemZoneId));
  }

  @Test
  public void zoned_timestamp_at() {
    ZoneId systemZoneId = ZoneId.systemDefault();
    insertAndRead(
        TableField.cql_timestamp,
        ZonedDateTime.now(systemZoneId).truncatedTo(ChronoUnit.MILLIS),
        ExtraTypeCodecs.zonedTimestampAt(systemZoneId));
  }

  private enum TestEnum {
    value1,
    value2,
    value3,
  }

  // Public for JSON serialization
  public static final class TestJsonAnnotatedPojo {
    public final String info;
    public final List<Integer> values;

    @JsonCreator
    public TestJsonAnnotatedPojo(
        @JsonProperty("info") String info, @JsonProperty("values") List<Integer> values) {
      this.info = info;
      this.values = values;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TestJsonAnnotatedPojo testJsonAnnotatedPojo = (TestJsonAnnotatedPojo) o;
      return Objects.equals(info, testJsonAnnotatedPojo.info)
          && Objects.equals(values, testJsonAnnotatedPojo.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(info, values);
    }
  }

  public static final class TestPojo {
    public int id;
    public String[] messages;

    public static TestPojo create(int id, String... messages) {
      TestPojo obj = new TestPojo();
      obj.id = id;
      obj.messages = messages;
      return obj;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TestPojo testPojo = (TestPojo) o;
      return id == testPojo.id && Arrays.equals(messages, testPojo.messages);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(id);
      result = 31 * result + Arrays.hashCode(messages);
      return result;
    }
  }
}
