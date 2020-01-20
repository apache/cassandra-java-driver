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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class CqlIdentifiers {

  @NonNull
  private static List<CqlIdentifier> wrap(
      @NonNull Iterable<String> in, @NonNull Function<String, CqlIdentifier> fn) {

    Objects.requireNonNull(in, "Input Iterable must not be null");
    Objects.requireNonNull(fn, "CqlIdentifier conversion function must not be null");
    ImmutableList.Builder<CqlIdentifier> builder = ImmutableList.builder();
    for (String name : in) {
      builder.add(fn.apply(name));
    }
    return builder.build();
  }

  @NonNull
  public static List<CqlIdentifier> wrap(@NonNull Iterable<String> in) {
    return wrap(in, CqlIdentifier::fromCql);
  }

  @NonNull
  public static List<CqlIdentifier> wrapInternal(@NonNull Iterable<String> in) {
    return wrap(in, CqlIdentifier::fromInternal);
  }

  @NonNull
  private static <V> Map<CqlIdentifier, V> wrapKeys(
      @NonNull Map<String, V> in, @NonNull Function<String, CqlIdentifier> fn) {
    Objects.requireNonNull(in, "Input Map must not be null");
    Objects.requireNonNull(fn, "CqlIdentifier conversion function must not be null");
    ImmutableMap.Builder<CqlIdentifier, V> builder = ImmutableMap.builder();
    for (Map.Entry<String, V> entry : in.entrySet()) {
      builder.put(fn.apply(entry.getKey()), entry.getValue());
    }
    return builder.build();
  }

  @NonNull
  public static <V> Map<CqlIdentifier, V> wrapKeys(@NonNull Map<String, V> in) {
    return wrapKeys(in, CqlIdentifier::fromCql);
  }

  @NonNull
  public static <V> Map<CqlIdentifier, V> wrapKeysInternal(@NonNull Map<String, V> in) {
    return wrapKeys(in, CqlIdentifier::fromInternal);
  }
}
