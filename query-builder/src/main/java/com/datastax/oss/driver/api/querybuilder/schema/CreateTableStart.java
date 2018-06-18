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

import edu.umd.cs.findbugs.annotations.NonNull;

public interface CreateTableStart extends OngoingPartitionKey {

  /**
   * Adds IF NOT EXISTS to the create table specification. This indicates that the table should not
   * be created if it already exists.
   */
  @NonNull
  CreateTableStart ifNotExists();
}
