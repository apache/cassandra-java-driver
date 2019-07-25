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

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The type of a Cassandra write query.
 *
 * <p>This information is returned by Cassandra when a write timeout is raised, to indicate what
 * type of write timed out. It is useful to decide which retry decision to adopt.
 *
 * <p>The only reason to model this as an interface (as opposed to an enum type) is to accommodate
 * for custom protocol extensions. If you're connecting to a standard Apache Cassandra cluster, all
 * {@code WriteType}s are {@link DefaultWriteType} instances.
 */
public interface WriteType {

  WriteType SIMPLE = DefaultWriteType.SIMPLE;
  WriteType BATCH = DefaultWriteType.BATCH;
  WriteType UNLOGGED_BATCH = DefaultWriteType.UNLOGGED_BATCH;
  WriteType COUNTER = DefaultWriteType.COUNTER;
  WriteType BATCH_LOG = DefaultWriteType.BATCH_LOG;
  WriteType CAS = DefaultWriteType.CAS;
  WriteType VIEW = DefaultWriteType.VIEW;
  WriteType CDC = DefaultWriteType.CDC;

  /** The textual representation that the write type is encoded to in protocol frames. */
  @NonNull
  String name();
}
