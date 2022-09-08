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
package com.datastax.oss.driver.api.querybuilder.select;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import org.junit.Test;

public class SelectUsingTest {
  private static final CqlDuration ONE_MS = CqlDuration.from("1ms");
  private static final CqlDuration TWO_MS = CqlDuration.from("2ms");

  @Test
  public void should_generate_timeout() {
    assertThat(selectFrom("foo").all().usingTimeout(CqlDuration.newInstance(0, 0, 1000000)))
        .hasCql("SELECT * FROM foo USING TIMEOUT 1ms");
  }

  @Test
  public void should_use_single_using_timeout_if_called_multiple_times() {
    assertThat(selectFrom("foo").all().usingTimeout(ONE_MS).usingTimeout(TWO_MS))
        .hasCql("SELECT * FROM foo USING TIMEOUT 2ms");
  }

  @Test
  public void should_clear_timeout() {
    assertThat(selectFrom("foo").all().usingTimeout(ONE_MS).usingTimeout((CqlDuration) null))
        .hasCql("SELECT * FROM foo");
    assertThat(selectFrom("foo").all().usingTimeout(TWO_MS).usingTimeout((BindMarker) null))
        .hasCql("SELECT * FROM foo");
  }
}
