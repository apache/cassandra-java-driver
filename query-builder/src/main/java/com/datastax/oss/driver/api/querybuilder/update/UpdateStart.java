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
package com.datastax.oss.driver.api.querybuilder.update;

import com.datastax.oss.driver.api.querybuilder.BindMarker;

/**
 * The beginning of an UPDATE statement. It needs at least one assignment before the WHERE clause
 * can be added.
 */
public interface UpdateStart extends OngoingAssignment {

  /**
   * Adds a USING TIMESTAMP clause to this statement with a literal value.
   *
   * <p>If this method or {@link #usingTimestamp(BindMarker)} is called multiple times, the last
   * value is used.
   */
  UpdateStart usingTimestamp(long timestamp);

  /**
   * Adds a USING TIMESTAMP clause to this statement with a bind marker.
   *
   * <p>If this method or {@link #usingTimestamp(long)} is called multiple times, the last value is
   * used.
   */
  UpdateStart usingTimestamp(BindMarker bindMarker);
}
