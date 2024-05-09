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

import org.junit.Test;

public class DeleteTimestampTest {

  @Test
  public void should_generate_using_timestamp_clause() {
    assertThat(deleteFrom("foo").usingTimestamp(1).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("DELETE FROM foo USING TIMESTAMP 1 WHERE k=?");
    assertThat(
            deleteFrom("foo").usingTimestamp(bindMarker()).whereColumn("k").isEqualTo(bindMarker()))
        .hasCql("DELETE FROM foo USING TIMESTAMP ? WHERE k=?");
    assertThat(
            deleteFrom("foo")
                .column("v")
                .usingTimestamp(1)
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("DELETE v FROM foo USING TIMESTAMP 1 WHERE k=?");
    assertThat(
            deleteFrom("foo")
                .column("v")
                .usingTimestamp(bindMarker())
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("DELETE v FROM foo USING TIMESTAMP ? WHERE k=?");
  }

  @Test
  public void should_use_last_timestamp_if_called_multiple_times() {
    assertThat(
            deleteFrom("foo")
                .usingTimestamp(1)
                .usingTimestamp(2)
                .usingTimestamp(3)
                .whereColumn("k")
                .isEqualTo(bindMarker()))
        .hasCql("DELETE FROM foo USING TIMESTAMP 3 WHERE k=?");
  }
}
