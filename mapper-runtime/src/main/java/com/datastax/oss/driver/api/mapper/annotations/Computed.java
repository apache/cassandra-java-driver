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
package com.datastax.oss.driver.api.mapper.annotations;

/**
 * Annotates the field or getter of an {@link Entity} property, to indicate that when retrieving
 * data that the property should be set to the result of computation on the Cassandra side,
 * typically a function call.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;Computed("writetime(v)")
 * private int writeTime;
 * </pre>
 */
public @interface Computed {

  /**
   * The formula used to compute the property.
   *
   * <p>This is a CQL expression like you would use directly in a query, for instance {@code
   * writetime(v)}.
   *
   * @return the formula.
   */
  String value();
}
