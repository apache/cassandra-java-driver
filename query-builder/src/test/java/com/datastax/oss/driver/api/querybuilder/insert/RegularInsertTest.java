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

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.api.querybuilder.insert;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.insert.DefaultInsert;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;

public class RegularInsertTest {

  @Test
  public void should_generate_column_assignments() {
    assertThat(insertInto("foo").value("a", literal(1)).value("b", literal(2)))
        .hasCql("INSERT INTO foo (a,b) VALUES (1,2)");
    assertThat(insertInto("ks", "foo").value("a", literal(1)).value("b", literal(2)))
        .hasCql("INSERT INTO ks.foo (a,b) VALUES (1,2)");
    assertThat(insertInto("foo").value("a", bindMarker()).value("b", bindMarker()))
        .hasCql("INSERT INTO foo (a,b) VALUES (?,?)");
  }

  @Test
  public void should_keep_last_assignment_if_column_listed_twice() {
    assertThat(
            insertInto("foo")
                .value("a", bindMarker())
                .value("b", bindMarker())
                .value("a", literal(1)))
        .hasCql("INSERT INTO foo (b,a) VALUES (?,1)");
  }

  @Test
  public void should_generate_bulk_column_assignments() {
    Map<String, Term> assignments = ImmutableMap.of("a", literal(1), "b", literal(2));
    assertThat(insertInto("ks", "foo").values(assignments))
        .hasCql("INSERT INTO ks.foo (a,b) VALUES (1,2)");

    assertThat(
            insertInto("ks", "foo")
                .value("a", literal(2))
                .value("c", literal(3))
                .values(assignments))
        .hasCql("INSERT INTO ks.foo (c,a,b) VALUES (3,1,2)");
  }

  @Test
  public void should_generate_if_not_exists_clause() {
    assertThat(insertInto("foo").value("a", bindMarker()).ifNotExists())
        .hasCql("INSERT INTO foo (a) VALUES (?) IF NOT EXISTS");
  }

  @Test
  public void should_generate_using_timestamp_clause() {
    assertThat(insertInto("foo").value("a", bindMarker()).usingTimestamp(1))
        .hasCql("INSERT INTO foo (a) VALUES (?) USING TIMESTAMP 1");
    assertThat(insertInto("foo").value("a", bindMarker()).usingTimestamp(bindMarker()))
        .hasCql("INSERT INTO foo (a) VALUES (?) USING TIMESTAMP ?");
  }

  @Test
  public void should_use_last_timestamp_if_called_multiple_times() {
    assertThat(
            insertInto("foo")
                .value("a", bindMarker())
                .usingTimestamp(1)
                .usingTimestamp(2)
                .usingTimestamp(3))
        .hasCql("INSERT INTO foo (a) VALUES (?) USING TIMESTAMP 3");
  }

  @Test
  public void should_generate_if_not_exists_and_timestamp_clauses() {
    assertThat(insertInto("foo").value("a", bindMarker()).ifNotExists().usingTimestamp(1))
        .hasCql("INSERT INTO foo (a) VALUES (?) IF NOT EXISTS USING TIMESTAMP 1");
  }

  @Test
  public void should_generate_ttl_clause() {
    assertThat(insertInto("foo").value("a", bindMarker()).usingTtl(10))
        .hasCql("INSERT INTO foo (a) VALUES (?) USING TTL 10");
  }

  @Test
  public void should_use_last_ttl_if_called_multiple_times() {
    assertThat(insertInto("foo").value("a", bindMarker()).usingTtl(10).usingTtl(20).usingTtl(30))
        .hasCql("INSERT INTO foo (a) VALUES (?) USING TTL 30");
  }

  @Test
  public void should_generate_using_timestamp_and_ttl_clauses() {
    assertThat(insertInto("foo").value("a", bindMarker()).usingTtl(10).usingTimestamp(30l))
        .hasCql("INSERT INTO foo (a) VALUES (?) USING TIMESTAMP 30 AND TTL 10");
    // order of TTL and TIMESTAMP method calls should not change the order of the generated clauses
    assertThat(insertInto("foo").value("a", bindMarker()).usingTimestamp(30l).usingTtl(10))
        .hasCql("INSERT INTO foo (a) VALUES (?) USING TIMESTAMP 30 AND TTL 10");
  }

  @Test
  public void should_throw_exception_with_invalid_ttl() {
    DefaultInsert defaultInsert =
        (DefaultInsert) insertInto("foo").value("a", bindMarker()).usingTtl(10);

    Throwable t =
        catchThrowable(
            () ->
                new DefaultInsert(
                    defaultInsert.getKeyspace(),
                    defaultInsert.getTable(),
                    (Term) defaultInsert.getJson(),
                    defaultInsert.getMissingJsonBehavior(),
                    defaultInsert.getAssignments(),
                    defaultInsert.getTimestamp(),
                    new Object(), // invalid TTL object,
                    defaultInsert.getTimeout(),
                    defaultInsert.isIfNotExists()));

    assertThat(t)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("TTL value must be a BindMarker or an Integer");
  }

  @Test
  public void should_throw_exception_with_invalid_timestamp() {
    DefaultInsert defaultInsert =
        (DefaultInsert) insertInto("foo").value("a", bindMarker()).usingTimestamp(1);

    Throwable t =
        catchThrowable(
            () ->
                new DefaultInsert(
                    defaultInsert.getKeyspace(),
                    defaultInsert.getTable(),
                    (Term) defaultInsert.getJson(),
                    defaultInsert.getMissingJsonBehavior(),
                    defaultInsert.getAssignments(),
                    new Object(), // invalid timestamp object)
                    defaultInsert.getTtlInSeconds(),
                    defaultInsert.getTimeout(),
                    defaultInsert.isIfNotExists()));
    assertThat(t)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("TIMESTAMP value must be a BindMarker or a Long");
  }
}
