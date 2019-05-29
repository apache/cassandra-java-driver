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
package com.datastax.oss.driver.api.querybuilder.insert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.core.CqlIdentifiers;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

public interface OngoingValues {

  /**
   * Sets a value for a column, as in {@code INSERT INTO ... (c) VALUES (?)}.
   *
   * <p>If this is called twice for the same column, the previous entry is discarded and the new
   * entry will be added at the end of the list.
   */
  @NonNull
  RegularInsert value(@NonNull CqlIdentifier columnId, @NonNull Term value);

  /**
   * Shortcut for {@link #value(CqlIdentifier, Term) value(CqlIdentifier.fromCql(columnName),
   * value)}.
   */
  @NonNull
  default RegularInsert value(@NonNull String columnName, @NonNull Term value) {
    return value(CqlIdentifier.fromCql(columnName), value);
  }

  /**
   * Sets values for multiple columns in one call, as in {@code INSERT INTO ... (key1, key2) VALUES
   * (value1, value2)}.
   *
   * <p>If the map contains columns that had already been added to this statement, the previous
   * entries are discarded and the new entries will be added at the end of the list.
   *
   * <p>Implementation note: this is a default method only for backward compatibility. The default
   * implementation calls {@link #value(CqlIdentifier, Term)} in a loop; it should be overridden if
   * a more efficient alternative exists.
   */
  @NonNull
  default RegularInsert valuesByIds(@NonNull Map<CqlIdentifier, Term> newAssignments) {
    if (newAssignments.isEmpty()) {
      throw new IllegalArgumentException("newAssignments can't be empty");
    }
    RegularInsert result = null;
    for (Map.Entry<CqlIdentifier, Term> entry : newAssignments.entrySet()) {
      result = (result == null ? this : result).value(entry.getKey(), entry.getValue());
    }
    return result;
  }

  /** Shortcut for {@link #valuesByIds(Map)} when the keys are plain strings. */
  @NonNull
  default RegularInsert values(@NonNull Map<String, Term> newAssignments) {
    return valuesByIds(CqlIdentifiers.wrapKeys(newAssignments));
  }
}
