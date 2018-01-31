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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CoreConsistencyLevel;
import com.google.common.collect.ImmutableList;

public class DefaultConsistencyLevelRegistry implements ConsistencyLevelRegistry {

  private static final ImmutableList<ConsistencyLevel> values =
      ImmutableList.<ConsistencyLevel>builder().add(CoreConsistencyLevel.values()).build();

  @Override
  public ConsistencyLevel fromCode(int code) {
    return CoreConsistencyLevel.fromCode(code);
  }

  @Override
  public ConsistencyLevel fromName(String name) {
    return CoreConsistencyLevel.valueOf(name);
  }

  @Override
  public Iterable<ConsistencyLevel> getValues() {
    return values;
  }
}
