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
package com.datastax.oss.driver.api.core.cql;

/**
 * The type of a batch.
 *
 * <p>The only reason to model this as an interface (as opposed to an enum type) is to accommodate
 * for custom protocol extensions. If you're connecting to a standard Apache Cassandra cluster, all
 * {@code BatchType}s are {@link DefaultBatchType} instances.
 */
public interface BatchType {

  BatchType LOGGED = DefaultBatchType.LOGGED;
  BatchType UNLOGGED = DefaultBatchType.UNLOGGED;
  BatchType COUNTER = DefaultBatchType.COUNTER;

  /** The numerical value that the batch type is encoded to. */
  byte getProtocolCode();

  // Implementation note: we don't have a "BatchTypeRegistry" because we never decode batch types.
  // This can be added later if needed (see ConsistencyLevelRegistry for an example).
}
