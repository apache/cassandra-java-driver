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
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

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
}
