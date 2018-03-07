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

public interface OngoingValues {

  /**
   * Sets a value for a column, as in {@code INSERT INTO ... (c) VALUES (?)}.
   *
   * <p>If this is called twice for the same column, the previous entry is discarded and the new
   * entry will remain at its current position.
   */
  RegularInsert value(CqlIdentifier columnId, Term value);

  /**
   * Shortcut for {@link #value(CqlIdentifier, Term) value(CqlIdentifier.fromCql(columnName),
   * value)}.
   */
  default RegularInsert value(String columnName, Term value) {
    return value(CqlIdentifier.fromCql(columnName), value);
  }
}
