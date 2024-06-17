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

import com.datastax.oss.driver.api.querybuilder.schema.compaction.SizeTieredCompactionStrategy;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultSizeTieredCompactionStrategy
    extends DefaultCompactionStrategy<DefaultSizeTieredCompactionStrategy>
    implements SizeTieredCompactionStrategy<DefaultSizeTieredCompactionStrategy> {

  public DefaultSizeTieredCompactionStrategy() {
    super("SizeTieredCompactionStrategy");
  }

  protected DefaultSizeTieredCompactionStrategy(@NonNull ImmutableMap<String, Object> options) {
    super(options);
  }

  @NonNull
  @Override
  public DefaultSizeTieredCompactionStrategy withOption(
      @NonNull String name, @NonNull Object value) {
    return new DefaultSizeTieredCompactionStrategy(
        ImmutableCollections.append(getInternalOptions(), name, value));
  }
}
