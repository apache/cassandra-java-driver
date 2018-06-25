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
package com.datastax.oss.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import edu.umd.cs.findbugs.annotations.NonNull;

/** A column in the schema metadata. */
public interface ColumnMetadata {

  @NonNull
  CqlIdentifier getKeyspace();

  /**
   * The identifier of the {@link TableMetadata} or a {@link ViewMetadata} that this column belongs
   * to.
   */
  @NonNull
  CqlIdentifier getParent();

  @NonNull
  CqlIdentifier getName();

  @NonNull
  DataType getType();

  boolean isStatic();
}
