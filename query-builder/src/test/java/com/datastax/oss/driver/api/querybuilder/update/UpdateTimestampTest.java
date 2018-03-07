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
import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.update;

import org.junit.Test;

public class UpdateTimestampTest {

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
}
