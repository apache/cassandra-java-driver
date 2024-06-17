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
package com.datastax.oss.driver.querybuilder;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.assertions.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@BackendRequirement(
    type = BackendType.CASSANDRA,
    minInclusive = "2.2",
    description = "JSON support in Cassandra was added in 2.2")
public class JsonInsertIT {
  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static final TypeCodec<User> JACKSON_JSON_CODEC = ExtraTypeCodecs.json(User.class);

  @BeforeClass
  public static void setup() {
    SESSION_RULE
        .session()
        .execute("CREATE TABLE json_jackson_row(id int PRIMARY KEY, name text, age int)");
  }

  @After
  public void clearTable() {
    SESSION_RULE.session().execute("TRUNCATE TABLE json_jackson_row");
  }

  @Test
  public void should_insert_string_as_json_using_simple_statement() {
    // given a simple statement
    try (CqlSession session = sessionWithCustomCodec()) {
      String jsonUser = "{ \"id\": 2, \"name\": \"Alice\", \"age\": 3 }";
      Statement stmt = insertInto("json_jackson_row").json(jsonUser).build();

      // when
      session.execute(stmt);

      // then
      String jsonUserResult =
          session
              .execute(selectFrom("json_jackson_row").json().all().build())
              .all()
              .get(0)
              .getString(0);

      assertThat(jsonUserResult).contains("\"id\": 2");
      assertThat(jsonUserResult).contains(" \"name\": \"Alice\"");
      assertThat(jsonUserResult).contains("\"age\": 3");
    }
  }

  @Test
  public void should_insert_json_using_prepare_statement() {
    // given prepare statement
    try (CqlSession session = sessionWithCustomCodec()) {
      User user = new User(2, "bob", 35);
      PreparedStatement pst =
          session.prepare(insertInto("json_jackson_row").json(bindMarker("user")).build());

      // when
      session.execute(pst.bind().set("user", user, User.class));

      // then
      List<Row> rows = session.execute(selectFrom("json_jackson_row").json().all().build()).all();
      assertThat(rows.get(0).get(0, User.class)).isEqualTo(user);
    }
  }

  @Test
  public void should_insert_json_using_simple_statement_with_custom_codec() {
    // given a simple statement
    try (CqlSession session = sessionWithCustomCodec()) {
      User user = new User(1, "alice", 30);
      Statement stmt = insertInto("json_jackson_row").json(user, JACKSON_JSON_CODEC).build();

      // when
      session.execute(stmt);

      // then
      List<Row> rows = session.execute(selectFrom("json_jackson_row").json().all().build()).all();

      assertThat(rows.get(0).get(0, User.class)).isEqualTo(user);
    }
  }

  @Test
  public void should_insert_json_using_simple_statement_with_custom_codec_without_codec_registry() {
    try (CqlSession session = sessionWithoutCustomCodec()) {
      // given
      User user = new User(1, "alice", 30);
      SimpleStatement stmt = insertInto("json_jackson_row").json(user, JACKSON_JSON_CODEC).build();

      // when
      session.execute(stmt);

      // then
      List<Row> rows = session.execute(selectFrom("json_jackson_row").json().all().build()).all();
      assertThat(rows.get(0).get(0, JACKSON_JSON_CODEC)).isEqualTo(user);
    }
  }

  @Test
  public void should_insert_json_using_simple_statement_with_codec_registry() {
    // given a simple statement
    try (CqlSession session = sessionWithCustomCodec()) {
      User user = new User(1, "alice", 30);
      Statement stmt =
          insertInto("json_jackson_row")
              .json(user, session.getContext().getCodecRegistry())
              .build();

      // when
      session.execute(stmt);

      // then
      List<Row> rows = session.execute(selectFrom("json_jackson_row").json().all().build()).all();

      assertThat(rows.get(0).get(0, User.class)).isEqualTo(user);
    }
  }

  @Test
  public void
      should_throw_when_insert_json_using_simple_statement_with_codec_registry_without_custom_codec() {
    assertThatThrownBy(
            () -> {
              try (CqlSession session = sessionWithoutCustomCodec()) {
                insertInto("json_jackson_row")
                    .json(new User(1, "alice", 30), session.getContext().getCodecRegistry())
                    .build();
              }
            })
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Could not inline JSON literal of type %s. "
                    + "This happens because the provided CodecRegistry does not contain "
                    + "a codec for this type. Try registering your TypeCodec in the registry "
                    + "first, or use json(Object, TypeCodec).",
                User.class.getName()))
        .hasCauseInstanceOf(CodecNotFoundException.class);
  }

  @SuppressWarnings("unchecked")
  private CqlSession sessionWithCustomCodec() {
    return (CqlSession)
        SessionUtils.baseBuilder()
            .addContactEndPoints(CCM_RULE.getContactPoints())
            .withKeyspace(SESSION_RULE.keyspace())
            .addTypeCodecs(JACKSON_JSON_CODEC)
            .build();
  }

  @SuppressWarnings("unchecked")
  private CqlSession sessionWithoutCustomCodec() {
    return (CqlSession)
        SessionUtils.baseBuilder()
            .addContactEndPoints(CCM_RULE.getContactPoints())
            .withKeyspace(SESSION_RULE.keyspace())
            .build();
  }

  @SuppressWarnings("unused")
  public static class User {

    private final int id;

    private final String name;

    private final int age;

    @JsonCreator
    public User(
        @JsonProperty("id") int id,
        @JsonProperty("name") String name,
        @JsonProperty("age") int age) {
      this.id = id;
      this.name = name;
      this.age = age;
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public int getAge() {
      return age;
    }

    @Override
    public String toString() {
      return String.format("%s (id %d, age %d)", name, id, age);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other instanceof User) {
        User that = (User) other;
        return this.id == that.id && this.age == that.age && Objects.equals(this.name, that.name);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, age);
    }
  }
}
