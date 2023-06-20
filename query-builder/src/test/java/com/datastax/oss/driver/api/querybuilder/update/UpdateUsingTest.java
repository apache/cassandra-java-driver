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
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.internal.querybuilder.update.DefaultUpdate;
import org.junit.Test;

public class UpdateUsingTest {

  @Test
  public void should_generate_using_timestamp_clause() {
    assertThat(
            update("foo")
                .usingTimestamp(1)
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo USING TIMESTAMP 1 SET v=? WHERE k=?");
    assertThat(
            update("foo")
                .usingTimestamp(bindMarker())
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo USING TIMESTAMP ? SET v=? WHERE k=?");
  }

  @Test
  public void should_use_last_timestamp_if_called_multiple_times() {
    assertThat(
            update("foo")
                .usingTimestamp(1)
                .usingTimestamp(2)
                .usingTimestamp(3)
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo USING TIMESTAMP 3 SET v=? WHERE k=?");
  }

  @Test
  public void should_generate_using_ttl_clause() {
    assertThat(
            update("foo")
                .usingTtl(10)
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo USING TTL 10 SET v=? WHERE k=?");
    assertThat(
            update("foo")
                .usingTtl(bindMarker())
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo USING TTL ? SET v=? WHERE k=?");
  }

  @Test
  public void should_use_last_ttl_if_called_multiple_times() {
    assertThat(
            update("foo")
                .usingTtl(10)
                .usingTtl(20)
                .usingTtl(30)
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo USING TTL 30 SET v=? WHERE k=?");
  }

  @Test
  public void should_generate_using_ttl_and_timestamp_clauses() {
    assertThat(
            update("foo")
                .usingTtl(10)
                .usingTimestamp(1)
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo USING TIMESTAMP 1 AND TTL 10 SET v=? WHERE k=?");
    // order of TTL and TIMESTAMP method calls should not change the order of the generated clauses
    assertThat(
            update("foo")
                .usingTimestamp(1)
                .usingTtl(10)
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo USING TIMESTAMP 1 AND TTL 10 SET v=? WHERE k=?");
    assertThat(
            update("foo")
                .usingTtl(bindMarker())
                .usingTimestamp(1)
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("UPDATE foo USING TIMESTAMP 1 AND TTL ? SET v=? WHERE k=?");
  }

  @Test
  public void should_throw_exception_with_invalid_ttl() {
    DefaultUpdate defaultUpdate =
        (DefaultUpdate)
            update("foo")
                .usingTtl(10)
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker());

    Throwable t =
        catchThrowable(
            () ->
                new DefaultUpdate(
                    defaultUpdate.getKeyspace(),
                    defaultUpdate.getTable(),
                    defaultUpdate.getTimestamp(),
                    new Object(), // invalid TTL object
                    defaultUpdate.getAssignments(),
                    defaultUpdate.getRelations(),
                    defaultUpdate.isIfExists(),
                    defaultUpdate.getConditions()));

    assertThat(t)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("TTL value must be a BindMarker or an Integer");
  }

  @Test
  public void should_throw_exception_with_invalid_timestamp() {
    DefaultUpdate defaultUpdate =
        (DefaultUpdate)
            update("foo")
                .usingTtl(10)
                .setColumn("v", bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker());

    Throwable t =
        catchThrowable(
            () ->
                new DefaultUpdate(
                    defaultUpdate.getKeyspace(),
                    defaultUpdate.getTable(),
                    new Object(), // invalid timestamp object
                    defaultUpdate.getTtl(),
                    defaultUpdate.getAssignments(),
                    defaultUpdate.getRelations(),
                    defaultUpdate.isIfExists(),
                    defaultUpdate.getConditions()));
    assertThat(t)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("TIMESTAMP value must be a BindMarker or a Long");
  }
}
