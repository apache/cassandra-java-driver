/*
 * Copyright (C) 2022 ScyllaDB
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
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class InsertUsingTimestampTest {
  final CqlDuration oneMs = CqlDuration.newInstance(0, 0, TimeUnit.MILLISECONDS.toNanos(1));
  final CqlDuration twoMs = CqlDuration.newInstance(0, 0, TimeUnit.MILLISECONDS.toNanos(2));

  @Test
  public void should_use_single_using_timeout_if_called_multiple_times() {
    assertThat(insertInto("foo").value("a", literal(1)).usingTimeout(oneMs).usingTimeout(twoMs))
        .hasCql("INSERT INTO foo (a) VALUES (1) USING TIMEOUT 2ms");
  }

  @Test
  public void should_clear_timeout() {
    assertThat(
            insertInto("foo")
                .value("a", literal(1))
                .usingTimeout(oneMs)
                .usingTimeout((CqlDuration) null))
        .hasCql("INSERT INTO foo (a) VALUES (1)");
    assertThat(
            insertInto("foo")
                .value("a", literal(1))
                .usingTimeout(oneMs)
                .usingTimeout((BindMarker) null))
        .hasCql("INSERT INTO foo (a) VALUES (1)");
  }
}
