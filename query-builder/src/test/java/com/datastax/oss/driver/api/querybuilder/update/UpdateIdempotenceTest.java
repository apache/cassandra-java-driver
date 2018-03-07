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
package com.datastax.oss.driver.api.querybuilder.update;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.function;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.raw;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.update;

import java.util.Arrays;
import org.junit.Test;

public class UpdateIdempotenceTest {

  @Test
  public void should_not_be_idempotent_if_conditional() {
    assertThat(update("foo").setColumn("v", bindMarker()).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET v=? WHERE k=?")
        .isIdempotent();
    assertThat(
            update("foo")
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker())
                .ifExists())
        .hasCql("UPDATE foo SET v=? WHERE k=? IF EXISTS")
        .isNotIdempotent();
    assertThat(
            update("foo")
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker())
                .ifColumn("v")
                .isEqualTo(literal(1)))
        .hasCql("UPDATE foo SET v=? WHERE k=? IF v=1")
        .isNotIdempotent();
  }

  @Test
  public void should_not_be_idempotent_if_assigning_non_idempotent_term() {
    assertThat(
            update("foo")
                .setColumn("v", function("non_idempotent_func"))
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET v=non_idempotent_func() WHERE k=?")
        .isNotIdempotent();
    assertThat(
            update("foo")
                .setColumn("v", raw("non_idempotent_func()"))
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET v=non_idempotent_func() WHERE k=?")
        .isNotIdempotent();
  }

  @Test
  public void should_not_be_idempotent_if_using_non_idempotent_term_in_relation() {
    assertThat(
            update("foo")
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(function("non_idempotent_func")))
        .hasCql("UPDATE foo SET v=? WHERE k=non_idempotent_func()")
        .isNotIdempotent();
    assertThat(
            update("foo")
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(raw("non_idempotent_func()")))
        .hasCql("UPDATE foo SET v=? WHERE k=non_idempotent_func()")
        .isNotIdempotent();
  }

  @Test
  public void should_not_be_idempotent_if_updating_counter() {
    assertThat(update("foo").increment("c").whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET c+=1 WHERE k=?")
        .isNotIdempotent();
    assertThat(update("foo").decrement("c").whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET c-=1 WHERE k=?")
        .isNotIdempotent();
  }

  @Test
  public void should_not_be_idempotent_if_adding_element_to_list() {
    assertThat(
            update("foo")
                .appendListElement("l", literal(1))
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET l+=[1] WHERE k=?")
        .isNotIdempotent();
    assertThat(
            update("foo")
                .prependListElement("l", literal(1))
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET l=[1]+l WHERE k=?")
        .isNotIdempotent();

    // On the other hand, other collections are safe:
    assertThat(
            update("foo")
                .appendSetElement("s", literal(1))
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET s+={1} WHERE k=?")
        .isIdempotent();
    assertThat(
            update("foo")
                .appendMapEntry("m", literal(1), literal("bar"))
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET m+={1:'bar'} WHERE k=?")
        .isIdempotent();
  }

  @Test
  public void should_not_be_idempotent_if_concatenating_to_collection() {
    assertThat(
            update("foo")
                .append("l", literal(Arrays.asList(1, 2, 3)))
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET l+=[1,2,3] WHERE k=?")
        .isNotIdempotent();
    assertThat(
            update("foo")
                .prepend("l", literal(Arrays.asList(1, 2, 3)))
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo SET l=[1,2,3]+l WHERE k=?")
        .isNotIdempotent();
  }
}
