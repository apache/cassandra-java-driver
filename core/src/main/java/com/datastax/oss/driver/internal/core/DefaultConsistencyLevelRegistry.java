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
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DefaultConsistencyLevelRegistry implements ConsistencyLevelRegistry {

  private static final ImmutableList<ConsistencyLevel> VALUES =
      ImmutableList.<ConsistencyLevel>builder().add(DefaultConsistencyLevel.values()).build();
  private static final ImmutableMap<String, Integer> NAME_TO_CODE;

  static {
    ImmutableMap.Builder<String, Integer> nameToCodeBuilder = ImmutableMap.builder();
    for (DefaultConsistencyLevel consistencyLevel : DefaultConsistencyLevel.values()) {
      nameToCodeBuilder.put(consistencyLevel.name(), consistencyLevel.getProtocolCode());
    }
    NAME_TO_CODE = nameToCodeBuilder.build();
  }

  @Override
  public ConsistencyLevel codeToLevel(int code) {
    return DefaultConsistencyLevel.fromCode(code);
  }

  @Override
  public int nameToCode(String name) {
    return NAME_TO_CODE.get(name);
  }

  @Override
  public ConsistencyLevel nameToLevel(String name) {
    return DefaultConsistencyLevel.valueOf(name);
  }

  @Override
  public Iterable<ConsistencyLevel> getValues() {
    return VALUES;
  }
}
