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
import com.datastax.oss.driver.internal.core.util.Loggers;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.List;

/**
 * A data structure where the values are accessible via a CQL identifier.
 *
 * <p>In the driver, these data structures are always accessible by index as well.
 */
public interface AccessibleById extends AccessibleByIndex {

  /**
   * Returns all the indices where a given identifier appears.
   *
   * @throws IllegalArgumentException if the id is invalid.
   * @apiNote the default implementation only exists for backward compatibility. It wraps the result
   *     of {@link #firstIndexOf(CqlIdentifier)} in a singleton list, which is not entirely correct,
   *     as it will only return the first occurrence. Therefore it also logs a warning.
   *     <p>Implementors should always override this method (all built-in driver implementations
   *     do).
   */
  @NonNull
  default List<Integer> allIndicesOf(@NonNull CqlIdentifier id) {
    Loggers.ACCESSIBLE_BY_ID.warn(
        "{} should override allIndicesOf(CqlIdentifier), the default implementation is a "
            + "workaround for backward compatibility, it only returns the first occurrence",
        getClass().getName());
    return Collections.singletonList(firstIndexOf(id));
  }

  /**
   * Returns the first index where a given identifier appears (depending on the implementation,
   * identifiers may appear multiple times).
   *
   * @throws IllegalArgumentException if the id is invalid.
   */
  int firstIndexOf(@NonNull CqlIdentifier id);

  /**
   * Returns the CQL type of the value for the first occurrence of {@code id}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IllegalArgumentException if the id is invalid.
   */
  @NonNull
  DataType getType(@NonNull CqlIdentifier id);
}
