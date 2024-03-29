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
package com.datastax.oss.driver.api.core.servererrors;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Thrown when a query attempts to create a keyspace or table that already exists.
 *
 * <p>This exception does not go through the {@link RetryPolicy}, it is always rethrown directly to
 * the client.
 */
public class AlreadyExistsException extends QueryValidationException {

  private final String keyspace;
  private final String table;

  public AlreadyExistsException(
      @NonNull Node coordinator, @NonNull String keyspace, @NonNull String table) {
    this(coordinator, makeMessage(keyspace, table), keyspace, table, null, false);
  }

  private AlreadyExistsException(
      @NonNull Node coordinator,
      @NonNull String message,
      @NonNull String keyspace,
      @NonNull String table,
      @Nullable ExecutionInfo executionInfo,
      boolean writableStackTrace) {
    super(coordinator, message, executionInfo, writableStackTrace);
    this.keyspace = keyspace;
    this.table = table;
  }

  private static String makeMessage(String keyspace, String table) {
    if (table == null || table.isEmpty()) {
      return String.format("Keyspace %s already exists", keyspace);
    } else {
      return String.format("Object %s.%s already exists", keyspace, table);
    }
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new AlreadyExistsException(
        getCoordinator(), getMessage(), keyspace, table, getExecutionInfo(), true);
  }
}
