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
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;

public class ImmutableCollections {

  @Nonnull
  public static <T> ImmutableList<T> append(@Nonnull ImmutableList<T> list, @Nonnull T newElement) {
    return ImmutableList.<T>builder().addAll(list).add(newElement).build();
  }

  @Nonnull
  public static <T> ImmutableList<T> concat(
      @Nonnull ImmutableList<T> list1, @Nonnull Iterable<T> list2) {
    return ImmutableList.<T>builder().addAll(list1).addAll(list2).build();
  }

  @Nonnull
  public static <T> ImmutableList<T> modifyLast(
      @Nonnull ImmutableList<T> list, @Nonnull Function<T, T> change) {
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
  @Nonnull
  public static <K, V> ImmutableMap<K, V> append(
      @Nonnull ImmutableMap<K, V> map, @Nonnull K newKey, @Nonnull V newValue) {
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
  @Nonnull
  public static <K, V> ImmutableMap<K, V> concat(
      @Nonnull ImmutableMap<K, V> map1, @Nonnull Map<K, V> map2) {
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
