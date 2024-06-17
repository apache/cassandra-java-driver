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
package com.datastax.oss.driver.api.core.data;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.util.Loggers;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.List;

/**
 * A data structure where the values are accessible via a name string.
 *
 * <p>This is an optimized version of {@link AccessibleById}, in case the overhead of having to
 * create a {@link CqlIdentifier} for each value is too much.
 *
 * <p>By default, case is ignored when matching names. If multiple names only differ by their case,
 * then the first one is chosen. You can force an exact match by double-quoting the name.
 *
 * <p>For example, if the data structure contains three values named {@code Foo}, {@code foo} and
 * {@code fOO}, then:
 *
 * <ul>
 *   <li>{@code getString("foo")} retrieves the first value (ignore case, first occurrence).
 *   <li>{@code getString("\"foo\"")} retrieves the second value (exact case).
 *   <li>{@code getString("\"fOO\"")} retrieves the third value (exact case).
 *   <li>{@code getString("\"FOO\"")} fails (exact case, no match).
 * </ul>
 *
 * <p>In the driver, these data structures are always accessible by index as well.
 */
public interface AccessibleByName extends AccessibleByIndex {

  /**
   * Returns all the indices where a given identifier appears.
   *
   * @throws IllegalArgumentException if the name is invalid.
   * @apiNote the default implementation only exists for backward compatibility. It wraps the result
   *     of {@link #firstIndexOf(String)} in a singleton list, which is not entirely correct, as it
   *     will only return the first occurrence. Therefore it also logs a warning.
   *     <p>Implementors should always override this method (all built-in driver implementations
   *     do).
   */
  @NonNull
  default List<Integer> allIndicesOf(@NonNull String name) {
    Loggers.ACCESSIBLE_BY_NAME.warn(
        "{} should override allIndicesOf(String), the default implementation is a "
            + "workaround for backward compatibility, it only returns the first occurrence",
        getClass().getName());
    return Collections.singletonList(firstIndexOf(name));
  }

  /**
   * Returns the first index where a given identifier appears (depending on the implementation,
   * identifiers may appear multiple times).
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  int firstIndexOf(@NonNull String name);

  /**
   * Returns the CQL type of the value for the first occurrence of {@code name}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * GettableByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  DataType getType(@NonNull String name);
}
