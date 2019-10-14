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
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class CqlIdentifiers {

  public static List<CqlIdentifier> wrap(
      @NonNull Iterable<String> in, @NonNull Function<String, CqlIdentifier> fn) {

    Preconditions.checkNotNull(in, "Input Iterable must not be null");
    Preconditions.checkNotNull(fn, "CqlIdentifier conversion function must not be null");
    ImmutableList.Builder<CqlIdentifier> out = ImmutableList.builder();
    for (String name : in) {
      out.add(fn.apply(name));
    }
    return out.build();
  }

  public static List<CqlIdentifier> wrap(Iterable<String> in) {
    return wrap(in, CqlIdentifier::fromCql);
  }

  public static List<CqlIdentifier> wrapInternal(Iterable<String> in) {
    return wrap(in, CqlIdentifier::fromInternal);
  }

  public static <V> Map<CqlIdentifier, V> wrapKeys(Map<String, V> in) {
    ImmutableMap.Builder<CqlIdentifier, V> out = ImmutableMap.builder();
    for (Map.Entry<String, V> entry : in.entrySet()) {
      out.put(CqlIdentifier.fromCql(entry.getKey()), entry.getValue());
    }
    return out.build();
  }
}
