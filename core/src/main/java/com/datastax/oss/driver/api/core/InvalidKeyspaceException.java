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

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import edu.umd.cs.findbugs.annotations.NonNull;

/** Thrown when a session gets created with an invalid keyspace. */
public class InvalidKeyspaceException extends DriverException {
  public InvalidKeyspaceException(@NonNull String message) {
    this(message, null);
  }

  private InvalidKeyspaceException(String message, ExecutionInfo executionInfo) {
    super(message, executionInfo, null, true);
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new InvalidKeyspaceException(getMessage(), getExecutionInfo());
  }
}
