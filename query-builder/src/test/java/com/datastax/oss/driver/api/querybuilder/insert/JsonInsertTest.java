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
package com.datastax.oss.driver.api.querybuilder.insert;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;

import org.junit.Test;

public class JsonInsertTest {

  @Test
  public void should_generate_insert_json() {
    assertThat(insertInto("foo").json("{\"bar\": 1}"))
        .hasCql("INSERT INTO foo JSON '{\"bar\": 1}'");
    assertThat(insertInto("foo").json(bindMarker())).hasCql("INSERT INTO foo JSON ?");
    assertThat(insertInto("foo").json(bindMarker()).defaultNull())
        .hasCql("INSERT INTO foo JSON ? DEFAULT NULL");
    assertThat(insertInto("foo").json(bindMarker()).defaultUnset())
        .hasCql("INSERT INTO foo JSON ? DEFAULT UNSET");
  }

  @Test
  public void should_keep_last_missing_json_behavior() {
    assertThat(insertInto("foo").json(bindMarker()).defaultNull().defaultUnset())
        .hasCql("INSERT INTO foo JSON ? DEFAULT UNSET");
  }

  @Test
  public void should_generate_if_not_exists_and_timestamp_clauses() {
    assertThat(insertInto("foo").json(bindMarker()).ifNotExists().usingTimestamp(1))
        .hasCql("INSERT INTO foo JSON ? IF NOT EXISTS USING TIMESTAMP 1");
    assertThat(insertInto("foo").json(bindMarker()).defaultUnset().ifNotExists().usingTimestamp(1))
        .hasCql("INSERT INTO foo JSON ? DEFAULT UNSET IF NOT EXISTS USING TIMESTAMP 1");
  }
}
