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
package com.datastax.oss.driver.api.core;

/**
 * A set of frequently used Apache Cassandra™ versions.
 *
 * @see Version
 */
public final class CassandraVersions {

  /** Apache Cassandra's version 2.0.0. */
  public static final Version CASSANDRA_2_0_0 = Version.parse("2.0.0");

  /** Apache Cassandra's version 2.1.0. */
  public static final Version CASSANDRA_2_1_0 = Version.parse("2.1.0");

  /** Apache Cassandra's version 2.2.0. */
  public static final Version CASSANDRA_2_2_0 = Version.parse("2.2.0");

  /** Apache Cassandra's version 3.0.0. */
  public static final Version CASSANDRA_3_0_0 = Version.parse("3.0.0");

  /** Apache Cassandra's version 4.0.0. */
  public static final Version CASSANDRA_4_0_0 = Version.parse("4.0.0");
}
