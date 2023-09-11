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

import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DefaultBatchTypeRegistry implements BatchTypeRegistry {
  private static final ImmutableList<BatchType> values =
      ImmutableList.<BatchType>builder().add(DefaultBatchType.values()).build();

  @Override
  public BatchType fromName(String name) {
    return DefaultBatchType.valueOf(name);
  }

  @Override
  public ImmutableList<BatchType> getValues() {
    return values;
  }
}
