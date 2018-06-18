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
package com.datastax.oss.driver.internal.querybuilder.schema;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;

public class Utils {
  /** Convenience method for creating a new {@link ImmutableSet} with an appended value. */
  @NonNull
  public static <E> ImmutableSet<E> appendSet(@NonNull ImmutableSet<E> set, @NonNull E newValue) {
    return ImmutableSet.<E>builder().addAll(set).add(newValue).build();
  }

  /** Convenience method for creating a new {@link ImmutableSet} with concatenated iterable. */
  @NonNull
  public static <E> ImmutableSet<E> concatSet(
      @NonNull ImmutableSet<E> set, @NonNull Iterable<E> toConcat) {
    return ImmutableSet.<E>builder().addAll(set).addAll(toConcat).build();
  }
}
