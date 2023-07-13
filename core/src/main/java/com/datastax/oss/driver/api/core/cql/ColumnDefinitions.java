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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.AccessibleByName;
import com.datastax.oss.driver.api.core.detach.Detachable;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Metadata about a set of CQL columns.
 *
 * <p>The default implementation returned by the driver is immutable and serializable. If you write
 * your own implementation, it should at least be thread-safe; serializability is not mandatory, but
 * recommended for use with some 3rd-party tools like Apache Spark &trade;.
 */
public interface ColumnDefinitions extends Iterable<ColumnDefinition>, Detachable {

  /** @return the number of definitions contained in this metadata. */
  int size();

  /**
   * @param i the index to check.
   * @throws IndexOutOfBoundsException if the index is invalid.
   * @return the {@code i}th {@link ColumnDefinition} in this metadata.
   */
  @NonNull
  ColumnDefinition get(int i);

  /**
   * Get a definition by name.
   *
   * <p>This is the equivalent of:
   *
   * <pre>
   *   get(firstIndexOf(name))
   * </pre>
   *
   * @throws IllegalArgumentException if the name does not exist (in other words, if {@code
   *     !contains(name))}).
   * @see #contains(String)
   * @see #firstIndexOf(String)
   */
  @NonNull
  default ColumnDefinition get(@NonNull String name) {
    if (!contains(name)) {
      throw new IllegalArgumentException("No definition named " + name);
    } else {
      return get(firstIndexOf(name));
    }
  }

  /**
   * Get a definition by name.
   *
   * <p>This is the equivalent of:
   *
   * <pre>
   *   get(firstIndexOf(name))
   * </pre>
   *
   * @throws IllegalArgumentException if the name does not exist (in other words, if {@code
   *     !contains(name))}).
   * @see #contains(CqlIdentifier)
   * @see #firstIndexOf(CqlIdentifier)
   */
  @NonNull
  default ColumnDefinition get(@NonNull CqlIdentifier name) {
    if (!contains(name)) {
      throw new IllegalArgumentException("No definition named " + name);
    } else {
      return get(firstIndexOf(name));
    }
  }

  /**
   * Whether there is a definition using the given name.
   *
   * <p>Because raw strings are ambiguous with regard to case-sensitivity, the argument will be
   * interpreted according to the rules described in {@link AccessibleByName}.
   */
  boolean contains(@NonNull String name);

  /** Whether there is a definition using the given CQL identifier. */
  boolean contains(@NonNull CqlIdentifier id);

  /**
   * Returns the index of the first column that uses the given name.
   *
   * <p>Because raw strings are ambiguous with regard to case-sensitivity, the argument will be
   * interpreted according to the rules described in {@link AccessibleByName}.
   *
   * <p>Also, note that if multiple columns use the same name, there is no way to find the index for
   * the next occurrences. One way to avoid this is to use aliases in your CQL queries.
   */
  int firstIndexOf(@NonNull String name);

  /**
   * Returns the index of the first column that uses the given identifier.
   *
   * <p>Note that if multiple columns use the same identifier, there is no way to find the index for
   * the next occurrences. One way to avoid this is to use aliases in your CQL queries.
   */
  int firstIndexOf(@NonNull CqlIdentifier id);
}
