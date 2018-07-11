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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.SchemaRefresh;

/**
 * The main entry point for system schema rows parsing.
 *
 * <p>Implementations must be thread-safe.
 */
public interface SchemaParser {

  /**
   * Process the rows that this parser was initialized with, and creates a refresh that will be
   * applied to the metadata.
   *
   * @see SchemaParserFactory#newInstance(SchemaRows)
   */
  SchemaRefresh parse();
}
