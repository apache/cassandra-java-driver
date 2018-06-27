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
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import java.util.concurrent.CompletionStage;

/**
 * Manages the queries to system tables during a schema refresh.
 *
 * <p>They are all asynchronous, and possibly paged. This class abstracts all the details and
 * exposes a common result type.
 *
 * <p>Implementations must be thread-safe.
 */
public interface SchemaQueries {

  /**
   * Launch the queries asynchronously, returning a future that will complete when they have all
   * succeeded.
   */
  CompletionStage<SchemaRows> execute();
}
