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
package com.datastax.oss.driver.internal.querybuilder;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.function.Function;

public class ImmutableCollections {

  @NonNull
  public static <T> ImmutableList<T> append(@NonNull ImmutableList<T> list, @NonNull T newElement) {
    return ImmutableList.<T>builder().addAll(list).add(newElement).build();
  }

  @NonNull
  public static <T> ImmutableList<T> concat(
      @NonNull ImmutableList<T> list1, @NonNull Iterable<T> list2) {
    return ImmutableList.<T>builder().addAll(list1).addAll(list2).build();
  }

  @NonNull
  public static <T> ImmutableList<T> modifyLast(
      @NonNull ImmutableList<T> list, @NonNull Function<T, T> change) {
    ImmutableList.Builder<T> builder = ImmutableList.builder();
    int size = list.size();
    for (int i = 0; i < size - 1; i++) {
      builder.add(list.get(i));
    }
    builder.add(change.apply(list.get(size - 1)));
    return builder.build();
  }

  /**
   * If the existing map has an entry with the new key, that old entry will be removed, but the new
   * entry will appear last in the iteration order of the resulting map. Example:
   *
   * <pre>{@code
   * append({a=>1, b=>2, c=>3}, a, 4) == {b=>2, c=>3, a=>4}
   * }</pre>
   */
  @NonNull
  public static <K, V> ImmutableMap<K, V> append(
      @NonNull ImmutableMap<K, V> map, @NonNull K newKey, @NonNull V newValue) {
    ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      if (!entry.getKey().equals(newKey)) {
        builder.put(entry);
      }
    }
    builder.put(newKey, newValue);
    return builder.build();
  }

  /**
   * If the existing map has entries that collide with the new map, those old entries will be
   * removed, but the new entries will appear at their new position in the iteration order of the
   * resulting map. Example:
   *
   * <pre>{@code
   * concat({a=>1, b=>2, c=>3}, {c=>4, a=>5}) == {b=>2, c=>4, a=>5}
   * }</pre>
   */
  @NonNull
  public static <K, V> ImmutableMap<K, V> concat(
      @NonNull ImmutableMap<K, V> map1, @NonNull Map<K, V> map2) {
    ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
    for (Map.Entry<K, V> entry : map1.entrySet()) {
      if (!map2.containsKey(entry.getKey())) {
        builder.put(entry);
      }
    }
    builder.putAll(map2);
    return builder.build();
  }
}
