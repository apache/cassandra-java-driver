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
package com.datastax.oss.driver.internal.core.data;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.AccessibleByName;
import com.datastax.oss.driver.api.core.data.GettableById;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.internal.core.util.Strings;
import com.datastax.oss.driver.shaded.guava.common.collect.LinkedListMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.ListMultimap;
import java.util.List;
import java.util.Locale;
import net.jcip.annotations.Immutable;

/**
 * Indexes an ordered list of identifiers.
 *
 * @see GettableByName
 * @see GettableById
 */
@Immutable
public class IdentifierIndex {

  private final ListMultimap<CqlIdentifier, Integer> byId;
  private final ListMultimap<String, Integer> byCaseSensitiveName;
  private final ListMultimap<String, Integer> byCaseInsensitiveName;

  public IdentifierIndex(List<CqlIdentifier> ids) {
    this.byId = LinkedListMultimap.create(ids.size());
    this.byCaseSensitiveName = LinkedListMultimap.create(ids.size());
    this.byCaseInsensitiveName = LinkedListMultimap.create(ids.size());

    int i = 0;
    for (CqlIdentifier id : ids) {
      byId.put(id, i);
      byCaseSensitiveName.put(id.asInternal(), i);
      byCaseInsensitiveName.put(id.asInternal().toLowerCase(Locale.ROOT), i);
      i += 1;
    }
  }

  /**
   * Returns all occurrences of a given name, given the matching rules described in {@link
   * AccessibleByName}.
   */
  public List<Integer> allIndicesOf(String name) {
    return Strings.isDoubleQuoted(name)
        ? byCaseSensitiveName.get(Strings.unDoubleQuote(name))
        : byCaseInsensitiveName.get(name.toLowerCase(Locale.ROOT));
  }

  /**
   * Returns the first occurrence of a given name, given the matching rules described in {@link
   * AccessibleByName}, or -1 if it's not in the list.
   */
  public int firstIndexOf(String name) {
    List<Integer> indices = allIndicesOf(name);
    return indices.isEmpty() ? -1 : indices.get(0);
  }

  /** Returns all occurrences of a given identifier. */
  public List<Integer> allIndicesOf(CqlIdentifier id) {
    return byId.get(id);
  }

  /** Returns the first occurrence of a given identifier, or -1 if it's not in the list. */
  public int firstIndexOf(CqlIdentifier id) {
    List<Integer> indices = allIndicesOf(id);
    return indices.isEmpty() ? -1 : indices.get(0);
  }
}
