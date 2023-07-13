/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The consistency level of a request.
 *
 * <p>The only reason to model this as an interface (as opposed to an enum type) is to accommodate
 * for custom protocol extensions. If you're connecting to a standard Apache Cassandra cluster, all
 * {@code ConsistencyLevel}s are {@link DefaultConsistencyLevel} instances.
 */
public interface ConsistencyLevel {

  /** The numerical value that the level is encoded to in protocol frames. */
  int getProtocolCode();

  /** The textual representation of the level in configuration files. */
  @NonNull
  String name();

  /** Whether this consistency level applies to the local datacenter only. */
  boolean isDcLocal();

  /**
   * Whether this consistency level is serial, that is, applies only to the "paxos" phase of a <a
   * href="https://docs.datastax.com/en/cassandra/3.0/cassandra/dml/dmlLtwtTransactions.html">lightweight
   * transaction</a>.
   *
   * <p>Serial consistency levels are only meaningful when executing conditional updates ({@code
   * INSERT}, {@code UPDATE} or {@code DELETE} statements with an {@code IF} condition).
   */
  boolean isSerial();
}
