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
package com.datastax.oss.driver.internal.querybuilder.schema.compaction;

import com.datastax.oss.driver.api.querybuilder.schema.compaction.CompactionStrategy;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public abstract class DefaultCompactionStrategy<SelfT extends DefaultCompactionStrategy<SelfT>>
    implements CompactionStrategy<SelfT> {

  private final ImmutableMap<String, Object> options;

  protected DefaultCompactionStrategy(@NonNull String className) {
    this(ImmutableMap.of("class", className));
  }

  protected DefaultCompactionStrategy(@NonNull ImmutableMap<String, Object> options) {
    this.options = options;
  }

  @NonNull
  public ImmutableMap<String, Object> getInternalOptions() {
    return options;
  }

  @NonNull
  @Override
  public Map<String, Object> getOptions() {
    return options;
  }
}
