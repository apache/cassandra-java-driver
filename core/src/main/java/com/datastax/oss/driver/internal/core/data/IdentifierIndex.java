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
package com.datastax.oss.driver.internal.core.data;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.AccessibleByName;
import com.datastax.oss.driver.api.core.data.GettableById;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.internal.core.util.Strings;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

/**
 * Indexes an ordered list of identifiers.
 *
 * @see GettableByName
 * @see GettableById
 */
public class IdentifierIndex {

  private final Map<CqlIdentifier, Integer> byId;
  private final Map<String, Integer> byCaseSensitiveName;
  private final Map<String, Integer> byCaseInsensitiveName;

  public IdentifierIndex(List<CqlIdentifier> ids) {
    this.byId = Maps.newHashMapWithExpectedSize(ids.size());
    this.byCaseSensitiveName = Maps.newHashMapWithExpectedSize(ids.size());
    this.byCaseInsensitiveName = Maps.newHashMapWithExpectedSize(ids.size());

    int i = 0;
    for (CqlIdentifier id : ids) {
      byId.putIfAbsent(id, i);
      byCaseSensitiveName.putIfAbsent(id.asInternal(), i);
      byCaseInsensitiveName.putIfAbsent(id.asInternal().toLowerCase(), i);
      i += 1;
    }
  }

  /**
   * Returns the first occurrence of a given name, given the matching rules described in {@link
   * AccessibleByName}, or -1 if it's not in the list.
   */
  public int firstIndexOf(String name) {
    Integer index =
        (Strings.isDoubleQuoted(name))
            ? byCaseSensitiveName.get(Strings.unDoubleQuote(name))
            : byCaseInsensitiveName.get(name.toLowerCase());
    return (index == null) ? -1 : index;
  }

  /** Returns the first occurrence of a given identifier, or -1 if it's not in the list. */
  public int firstIndexOf(CqlIdentifier id) {
    Integer index = byId.get(id);
    return (index == null) ? -1 : index;
  }
}
