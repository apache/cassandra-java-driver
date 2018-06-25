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
package com.datastax.oss.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface CreateMaterializedViewPrimaryKeyStart {

  /**
   * Adds a partition key to primary key definition.
   *
   * <p>Partition keys are added in the order of their declaration.
   */
  @NonNull
  CreateMaterializedViewPrimaryKey withPartitionKey(@NonNull CqlIdentifier columnName);

  /**
   * Shortcut for {@link #withPartitionKey(CqlIdentifier)
   * withPartitionKey(CqlIdentifier.asCql(columnName)}.
   */
  @NonNull
  default CreateMaterializedViewPrimaryKey withPartitionKey(@NonNull String columnName) {
    return withPartitionKey(CqlIdentifier.fromCql(columnName));
  }
}
