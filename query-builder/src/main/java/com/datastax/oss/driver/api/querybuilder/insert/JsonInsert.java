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
package com.datastax.oss.driver.api.querybuilder.insert;

import edu.umd.cs.findbugs.annotations.NonNull;

/** An INSERT JSON statement. */
public interface JsonInsert extends Insert {

  /**
   * Adds a DEFAULT NULL clause to this statement.
   *
   * <p>If this or {@link #defaultUnset()} is called multiple times, the last value is used.
   */
  @NonNull
  JsonInsert defaultNull();

  /**
   * Adds a DEFAULT UNSET clause to this statement.
   *
   * <p>If this or {@link #defaultNull()} is called multiple times, the last value is used.
   */
  @NonNull
  JsonInsert defaultUnset();
}
