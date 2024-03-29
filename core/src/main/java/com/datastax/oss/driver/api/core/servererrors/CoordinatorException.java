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
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/** A server-side error thrown by the coordinator node in response to a driver request. */
public abstract class CoordinatorException extends DriverException {

  // This is also present on ExecutionInfo. But the execution info is only set for errors that are
  // rethrown to the client, not on errors that get retried. It can be useful to know the node in
  // the retry policy, so store it here, it might be duplicated but that doesn't matter.
  private final Node coordinator;

  protected CoordinatorException(
      @NonNull Node coordinator,
      @NonNull String message,
      @Nullable ExecutionInfo executionInfo,
      boolean writableStackTrace) {
    super(message, executionInfo, null, writableStackTrace);
    this.coordinator = coordinator;
  }

  @NonNull
  public Node getCoordinator() {
    return coordinator;
  }
}
