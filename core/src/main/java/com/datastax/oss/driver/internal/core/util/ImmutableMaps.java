/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;

public class ImmutableMaps {

  /** Returns a new immutable map that overrides one mapping in the source map. */
  public static <K, V> ImmutableMap<K, V> replace(Map<K, V> source, K key, V newValue) {
    return ImmutableMap.<K, V>builder()
        .put(key, newValue)
        .putAll(Maps.filterKeys(source, k -> !key.equals(k)))
        .build();
  }
}
