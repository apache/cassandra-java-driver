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
package com.datastax.oss.driver.internal.core;

/** Features of the native protocol that are only supported in specific versions. */
public enum ProtocolFeature {

  /**
   * The ability to leave variables unset in prepared statements.
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-7304">CASSANDRA-7304</a>
   */
  UNSET_BOUND_VALUES,
  ;
}
