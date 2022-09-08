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

import org.junit.Test;

public class SelectBypassCacheTest {
  @Test
  public void should_generate_bypass_cache() {
    assertThat(selectFrom("foo").all().bypassCache()).hasCql("SELECT * FROM foo BYPASS CACHE");
  }

  @Test
  public void should_use_single_bypass_cache_if_called_multiple_times() {
    assertThat(selectFrom("foo").all().bypassCache().bypassCache())
        .hasCql("SELECT * FROM foo BYPASS CACHE");
  }
}
