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
package com.datastax.oss.driver.api.core.data;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;

/**
 * A data structure where the values are accessible via an integer index, a name, or a CQL
 * identifier.
 */
public interface Accessible extends AccessibleByIndex, AccessibleByName, AccessibleById {

  /**
   * Returns the first index where a given identifier appears (depending on the implementation,
   * identifiers may appear multiple times).
   */
  int firstIndexOf(CqlIdentifier id);

  /**
   * Returns the first index where a given identifier appears (depending on the implementation,
   * identifiers may appear multiple times).
   */
  int firstIndexOf(String name);

  @Override
  default DataType getType(CqlIdentifier id) {
    return getType(firstIndexOf(id));
  }

  @Override
  default DataType getType(String name) {
    return getType(firstIndexOf(name));
  }
}
