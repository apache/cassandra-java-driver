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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.oss.driver.shaded.guava.common.base.Objects;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.io.Serializable;
import net.jcip.annotations.Immutable;

/**
 * A container for a term and maximum edit distance.
 *
 * <p>The context in which this is used determines the semantics of the edit distance. For instance,
 * it might indicate single-character edits if used with fuzzy search queries or whole word
 * movements if used with phrase proximity queries.
 */
@Immutable
public class EditDistance implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final int DEFAULT_EDIT_DISTANCE = 0;

  public final String query;
  public final int distance;

  public EditDistance(String query) {
    this(query, DEFAULT_EDIT_DISTANCE);
  }

  public EditDistance(String query, int distance) {
    Preconditions.checkNotNull(query, "Query cannot be null.");
    Preconditions.checkArgument(distance >= 0, "Edit distance cannot be negative.");
    this.query = query;
    this.distance = distance;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EditDistance)) {
      return false;
    }
    EditDistance that = (EditDistance) o;
    return distance == that.distance && Objects.equal(query, that.query);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(query, distance);
  }

  @Override
  public String toString() {
    return "EditDistance{" + "query='" + query + '\'' + ", distance=" + distance + '}';
  }
}
