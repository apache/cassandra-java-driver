package com.datastax.oss.driver.api.querybuilder;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.assertions.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class JsonInsertIT {
  private static final CcmRule ccmRule = CcmRule.getInstance();
  private static final JacksonJsonCodec<User> JACKSON_JSON_CODEC =
      new JacksonJsonCodec<>(User.class);

  private static SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  .build())
          .build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @BeforeClass
  public static void setup() {
    sessionRule
        .session()
        .execute("CREATE TABLE json_jackson_row(id int PRIMARY KEY, name text, age int)");
  }

  @Test
  public void should_insert_json_object_a_a_json_row_using_prepare_statement() {
    // given prepare statement
    try (CqlSession session = sessionWithCustomCodec()) {
      User user = new User(2, "bob", 35);
      PreparedStatement pst =
          session.prepare(insertInto("json_jackson_row").json(bindMarker("user")).build());

      // when
      session.execute(pst.bind().set("user", user, User.class));

      // then
      List<Row> rows = session.execute(selectFrom("json_jackson_row").json().all().build()).all();
      Assertions.assertThat(rows.get(0).get(0, User.class)).isEqualTo(user);
    }
  }

  @Test
  public void should_insert_json_object_as_a_json_row_using_simple_statement_with_custom_codec() {
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
  public void should_insert_json_object_as_a_json_row_using_simple_statement_with_codec_registry() {
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

  @SuppressWarnings("unchecked")
  private CqlSession sessionWithCustomCodec() {
    return (CqlSession)
        SessionUtils.baseBuilder()
            .addContactPoints(ccmRule.getContactPoints())
            .withKeyspace(sessionRule.keyspace())
            .addTypeCodecs(JACKSON_JSON_CODEC)
            .build();
  }

  @SuppressWarnings("unchecked")
  private CqlSession sessionWithoutCustomCodec() {
    return (CqlSession)
        SessionUtils.baseBuilder()
            .addContactPoints(ccmRule.getContactPoints())
            .withKeyspace(sessionRule.keyspace())
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
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      User user = (User) o;
      return id == user.id && age == user.age && Objects.equals(name, user.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, age);
    }
  }
}
