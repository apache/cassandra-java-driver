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
 * Indicates that the contacted node reported a protocol error.
 *
 * <p>Protocol errors indicate that the client triggered a protocol violation (for instance, a
 * {@code QUERY} message is sent before a {@code STARTUP} one has been sent). Protocol errors should
 * be considered as a bug in the driver and reported as such.
 *
 * <p>This exception does not go through the {@link RetryPolicy}, it is always rethrown directly to
 * the client.
 */
public class ProtocolError extends CoordinatorException {

  public ProtocolError(@NonNull Node coordinator, @NonNull String message) {
    this(coordinator, message, null, false);
  }

  private ProtocolError(
      @NonNull Node coordinator,
      @NonNull String message,
      @Nullable ExecutionInfo executionInfo,
      boolean writableStackTrace) {
    super(coordinator, message, executionInfo, writableStackTrace);
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new ProtocolError(getCoordinator(), getMessage(), getExecutionInfo(), true);
  }
}
