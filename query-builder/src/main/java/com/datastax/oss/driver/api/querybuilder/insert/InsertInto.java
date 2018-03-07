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

import com.datastax.oss.driver.api.querybuilder.BindMarker;

/**
 * The beginning of an INSERT statement; at this point only the table is known, it might become a
 * JSON insert or a regular one, depending on which method is called next.
 */
public interface InsertInto extends OngoingValues {

  /** Makes this statement an INSERT JSON with the provided JSON string. */
  JsonInsert json(String json);

  /** Makes this statement an INSERT JSON with a bind marker, as in {@code INSERT JSON ?}. */
  JsonInsert json(BindMarker bindMarker);
}
