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
package com.datastax.oss.driver.api.core.servererrors;

/**
 * The type of a Cassandra write query.
 *
 * <p>This information is returned by Cassandra when a write timeout is raised, to indicate what
 * type of write timed out. It is useful to decide which retry decision to adopt.
 *
 * <p>The only reason to model this as an interface (as opposed to an enum type) is to accommodate
 * for custom protocol extensions. If you're connecting to a standard Apache Cassandra cluster, all
 * {@code WriteType}s are {@link CoreWriteType} instances.
 */
public interface WriteType {

  /** The textual representation that the write type is encoded to in protocol frames. */
  String name();
}
