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
package com.datastax.oss.driver.api.querybuilder.insert;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.add;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.function;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.raw;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;

import org.junit.Test;

public class InsertIdempotenceTest {

  @Test
  public void should_not_be_idempotent_if_conditional() {
    assertThat(insertInto("foo").value("k", literal(1)))
        .hasCql("INSERT INTO foo (k) VALUES (1)")
        .isIdempotent();
    assertThat(insertInto("foo").value("k", literal(1)).ifNotExists())
        .hasCql("INSERT INTO foo (k) VALUES (1) IF NOT EXISTS")
        .isNotIdempotent();
  }

  @Test
  public void should_not_be_idempotent_if_inserting_non_idempotent_term() {
    assertThat(insertInto("foo").value("k", literal(1)))
        .hasCql("INSERT INTO foo (k) VALUES (1)")
        .isIdempotent();
    assertThat(insertInto("foo").value("k", function("generate_id")))
        .hasCql("INSERT INTO foo (k) VALUES (generate_id())")
        .isNotIdempotent();
    assertThat(insertInto("foo").value("k", raw("generate_id()")))
        .hasCql("INSERT INTO foo (k) VALUES (generate_id())")
        .isNotIdempotent();

    assertThat(insertInto("foo").value("k", add(literal(1), literal(1))))
        .hasCql("INSERT INTO foo (k) VALUES (1+1)")
        .isIdempotent();
    assertThat(insertInto("foo").value("k", add(literal(1), function("generate_id"))))
        .hasCql("INSERT INTO foo (k) VALUES (1+generate_id())")
        .isNotIdempotent();

    assertThat(insertInto("foo").value("k", tuple(literal(1), literal(1))))
        .hasCql("INSERT INTO foo (k) VALUES ((1,1))")
        .isIdempotent();
    assertThat(insertInto("foo").value("k", tuple(literal(1), function("generate_id"))))
        .hasCql("INSERT INTO foo (k) VALUES ((1,generate_id()))")
        .isNotIdempotent();
  }
}
