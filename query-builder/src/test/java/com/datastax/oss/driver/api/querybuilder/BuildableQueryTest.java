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
package com.datastax.oss.driver.api.querybuilder;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.function;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.subCondition;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class BuildableQueryTest {

  @DataProvider
  public static Object[][] sampleQueries() {
    // query | values | expected CQL | expected idempotence
    return new Object[][] {
      {
        selectFrom("foo").all().whereColumn("k").isEqualTo(bindMarker("k")),
        ImmutableMap.of("k", 1),
        "SELECT * FROM foo WHERE k=:k",
        true
      },
      {
        selectFrom("foo")
            .all()
            .whereColumn("k")
            .isEqualTo(bindMarker("k"))
            .or()
            .whereColumn("l")
            .isEqualTo(bindMarker("l")),
        ImmutableMap.of("k", 1, "l", 2),
        "SELECT * FROM foo WHERE k=:k OR l=:l",
        true
      },
      {
        selectFrom("foo")
            .all()
            .whereColumn("k")
            .isEqualTo(bindMarker("k"))
            .and()
            .where(
                subCondition()
                    .whereColumn("l")
                    .isEqualTo(bindMarker("l"))
                    .or()
                    .whereColumn("m")
                    .isEqualTo(bindMarker("m")))
            .whereColumn("n")
            .isEqualTo(bindMarker("n")),
        ImmutableMap.of("k", 1, "l", 2, "m", 3, "n", 4),
        "SELECT * FROM foo WHERE k=:k AND (l=:l OR m=:m) AND n=:n",
        true
      },
      {
        deleteFrom("foo").whereColumn("k").isEqualTo(bindMarker("k")),
        ImmutableMap.of("k", 1),
        "DELETE FROM foo WHERE k=:k",
        true
      },
      {
        deleteFrom("foo").whereColumn("k").isEqualTo(bindMarker("k")).ifExists(),
        ImmutableMap.of("k", 1),
        "DELETE FROM foo WHERE k=:k IF EXISTS",
        false
      },
      {
        insertInto("foo").value("a", bindMarker("a")).value("b", bindMarker("b")),
        ImmutableMap.of("a", 1, "b", "b"),
        "INSERT INTO foo (a,b) VALUES (:a,:b)",
        true
      },
      {
        insertInto("foo").value("k", tuple(bindMarker("field1"), function("generate_id"))),
        ImmutableMap.of("field1", 1),
        "INSERT INTO foo (k) VALUES ((:field1,generate_id()))",
        false
      },
      {
        update("foo").setColumn("v", bindMarker("v")).whereColumn("k").isEqualTo(bindMarker("k")),
        ImmutableMap.of("v", 3, "k", 1),
        "UPDATE foo SET v=:v WHERE k=:k",
        true
      },
      {
        update("foo")
            .setColumn("v", function("non_idempotent_func"))
            .whereColumn("k")
            .isEqualTo(bindMarker("k")),
        ImmutableMap.of("k", 1),
        "UPDATE foo SET v=non_idempotent_func() WHERE k=:k",
        false
      },
    };
  }

  @Test
  @UseDataProvider("sampleQueries")
  public void should_build_statement_without_values(
      BuildableQuery query,
      @SuppressWarnings("unused") Map<String, Object> boundValues,
      String expectedQueryString,
      boolean expectedIdempotence) {
    SimpleStatement statement = query.build();
    assertThat(statement.getQuery()).isEqualTo(expectedQueryString);
    assertThat(statement.isIdempotent()).isEqualTo(expectedIdempotence);
    assertThat(statement.getPositionalValues()).isEmpty();
    assertThat(statement.getNamedValues()).isEmpty();
  }

  @Test
  @UseDataProvider("sampleQueries")
  public void should_build_statement_with_positional_values(
      BuildableQuery query,
      Map<String, Object> boundValues,
      String expectedQueryString,
      boolean expectedIdempotence) {
    Object[] positionalValues = boundValues.values().toArray();
    SimpleStatement statement = query.build(positionalValues);
    assertThat(statement.getQuery()).isEqualTo(expectedQueryString);
    assertThat(statement.isIdempotent()).isEqualTo(expectedIdempotence);
    assertThat(statement.getPositionalValues()).containsExactly(positionalValues);
    assertThat(statement.getNamedValues()).isEmpty();
  }

  @Test
  @UseDataProvider("sampleQueries")
  public void should_build_statement_with_named_values(
      BuildableQuery query,
      Map<String, Object> boundValues,
      String expectedQueryString,
      boolean expectedIdempotence) {
    SimpleStatement statement = query.build(boundValues);
    assertThat(statement.getQuery()).isEqualTo(expectedQueryString);
    assertThat(statement.isIdempotent()).isEqualTo(expectedIdempotence);
    assertThat(statement.getPositionalValues()).isEmpty();
    assertThat(statement.getNamedValues()).hasSize(boundValues.size());
    for (Map.Entry<String, Object> entry : boundValues.entrySet()) {
      assertThat(statement.getNamedValues().get(CqlIdentifier.fromCql(entry.getKey())))
          .isEqualTo(entry.getValue());
    }
  }

  @Test
  @UseDataProvider("sampleQueries")
  public void should_convert_to_statement_builder(
      BuildableQuery query,
      Map<String, Object> boundValues,
      String expectedQueryString,
      boolean expectedIdempotence) {
    Object[] positionalValues = boundValues.values().toArray();
    SimpleStatement statement = query.builder().addPositionalValues(positionalValues).build();
    assertThat(statement.getQuery()).isEqualTo(expectedQueryString);
    assertThat(statement.isIdempotent()).isEqualTo(expectedIdempotence);
    assertThat(statement.getPositionalValues()).containsExactly(positionalValues);
    assertThat(statement.getNamedValues()).isEmpty();
  }
}
