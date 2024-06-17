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
package com.datastax.oss.driver.api.querybuilder.delete;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.function;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.raw;

import org.junit.Test;

public class DeleteIdempotenceTest {

  @Test
  public void should_not_be_idempotent_if_conditional() {
    assertThat(deleteFrom("foo").whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("DELETE FROM foo WHERE k=?")
        .isIdempotent();
    assertThat(deleteFrom("foo").whereColumn("k").isEqualTo(bindMarker()).ifExists())
        .hasCql("DELETE FROM foo WHERE k=? IF EXISTS")
        .isNotIdempotent();
    assertThat(
            deleteFrom("foo")
                .whereColumn("k")
                .isEqualTo(bindMarker())
                .ifColumn("k")
                .isEqualTo(literal(1)))
        .hasCql("DELETE FROM foo WHERE k=? IF k=1")
        .isNotIdempotent();
  }

  @Test
  public void should_not_be_idempotent_if_deleting_collection_element() {
    assertThat(deleteFrom("foo").element("l", literal(0)).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("DELETE l[0] FROM foo WHERE k=?")
        .isNotIdempotent();
  }

  @Test
  public void should_not_be_idempotent_if_using_non_idempotent_term_in_relation() {
    assertThat(deleteFrom("foo").whereColumn("k").isEqualTo(function("non_idempotent_func")))
        .hasCql("DELETE FROM foo WHERE k=non_idempotent_func()")
        .isNotIdempotent();
    assertThat(deleteFrom("foo").whereColumn("k").isEqualTo(raw("1")))
        .hasCql("DELETE FROM foo WHERE k=1")
        .isNotIdempotent();
  }
}
