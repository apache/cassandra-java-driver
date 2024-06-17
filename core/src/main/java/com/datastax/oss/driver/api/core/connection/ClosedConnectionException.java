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
package com.datastax.oss.driver.api.core.connection;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverException;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Thrown when the connection on which a request was executing is closed due to an unrelated event.
 *
 * <p>For example, this can happen if the node is unresponsive and a heartbeat query failed, or if
 * the node was forced down.
 *
 * <p>The driver will retry these requests on the next node transparently, unless the request is not
 * idempotent. Therefore, this exception is usually observed as part of an {@link
 * AllNodesFailedException}.
 */
public class ClosedConnectionException extends DriverException {

  public ClosedConnectionException(@NonNull String message) {
    this(message, null, false);
  }

  public ClosedConnectionException(@NonNull String message, @Nullable Throwable cause) {
    this(message, cause, false);
  }

  private ClosedConnectionException(
      @NonNull String message, @Nullable Throwable cause, boolean writableStackTrace) {
    super(message, null, cause, writableStackTrace);
  }

  @Override
  @NonNull
  public DriverException copy() {
    return new ClosedConnectionException(getMessage(), getCause(), true);
  }
}
