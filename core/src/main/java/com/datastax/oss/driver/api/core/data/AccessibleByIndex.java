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
package com.datastax.oss.driver.api.core.data;

import com.datastax.oss.driver.api.core.type.DataType;
import edu.umd.cs.findbugs.annotations.NonNull;

/** A data structure where the values are accessible via an integer index. */
public interface AccessibleByIndex extends Data {

  /** Returns the number of values. */
  int size();

  /**
   * Returns the CQL type of the {@code i}th value.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @NonNull
  DataType getType(int i);
}
