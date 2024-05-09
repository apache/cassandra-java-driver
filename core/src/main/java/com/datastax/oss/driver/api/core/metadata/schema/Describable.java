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
package com.datastax.oss.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import edu.umd.cs.findbugs.annotations.NonNull;

/** A schema element that can be described in terms of CQL {@code CREATE} statements. */
public interface Describable {

  /**
   * Returns a single CQL statement that creates the element.
   *
   * @param pretty if {@code true}, make the output more human-readable (line breaks, indents, and
   *     {@link CqlIdentifier#asCql(boolean) pretty identifiers}). If {@code false}, return the
   *     statement on a single line with minimal formatting.
   */
  @NonNull
  String describe(boolean pretty);

  /**
   * Returns a CQL script that creates the element and all of its children. For example: a schema
   * with its tables, materialized views, types, etc. A table with its indices.
   *
   * @param pretty if {@code true}, make the output more human-readable (line breaks, indents, and
   *     {@link CqlIdentifier#asCql(boolean) pretty identifiers}). If {@code false}, return each
   *     statement on a single line with minimal formatting.
   */
  @NonNull
  String describeWithChildren(boolean pretty);
}
