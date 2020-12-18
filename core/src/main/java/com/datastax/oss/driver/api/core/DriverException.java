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
import com.datastax.oss.driver.api.core.cql.ResultSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Base class for all exceptions thrown by the driver.
 *
 * <p>Note that, for obvious programming errors, the driver might throw JDK runtime exceptions, such
 * as {@link IllegalArgumentException} or {@link IllegalStateException}. In all other cases, it will
 * be an instance of this class.
 *
 * <p>One special case is when the driver tried multiple nodes to complete a request, and they all
 * failed; the error returned to the client will be an {@link AllNodesFailedException}, which wraps
 * a map of errors per node.
 *
 * <p>Some implementations make the stack trace not writable to improve performance (see {@link
 * Throwable#Throwable(String, Throwable, boolean, boolean)}). This is only done when the exception
 * is thrown in a small number of well-known cases, and the stack trace wouldn't add any useful
 * information (for example, server error responses). Instances returned by {@link #copy()} always
 * have a stack trace.
 */
public abstract class DriverException extends RuntimeException {

  private transient volatile ExecutionInfo executionInfo;

  protected DriverException(
      @Nullable String message,
      @Nullable ExecutionInfo executionInfo,
      @Nullable Throwable cause,
      boolean writableStackTrace) {
    super(message, cause, true, writableStackTrace);
    this.executionInfo = executionInfo;
  }

  /**
   * Returns execution information about the request that led to this error.
   *
   * <p>This is similar to the information returned for a successful query in {@link ResultSet},
   * except that some fields may be absent:
   *
   * <ul>
   *   <li>{@link ExecutionInfo#getCoordinator()} may be null if the error occurred before any node
   *       was contacted;
   *   <li>{@link ExecutionInfo#getErrors()} will contain the errors encountered for other nodes,
   *       but not this error itself;
   *   <li>{@link ExecutionInfo#getSuccessfulExecutionIndex()} may be -1 if the error occurred
   *       before any execution was started;
   *   <li>{@link ExecutionInfo#getPagingState()} and {@link ExecutionInfo#getTracingId()} will
   *       always be null;
   *   <li>{@link ExecutionInfo#getWarnings()} and {@link ExecutionInfo#getIncomingPayload()} will
   *       always be empty;
   *   <li>{@link ExecutionInfo#isSchemaInAgreement()} will always be true;
   *   <li>{@link ExecutionInfo#getResponseSizeInBytes()} and {@link
   *       ExecutionInfo#getCompressedResponseSizeInBytes()} will always be -1.
   * </ul>
   *
   * <p>Note that this is only set for exceptions that are rethrown directly to the client from a
   * session call. For example, individual node errors stored in {@link
   * AllNodesFailedException#getAllErrors()} or {@link ExecutionInfo#getErrors()} do not contain
   * their own execution info, and therefore return null from this method.
   *
   * <p>This method will also return null for low-level exceptions thrown directly from a driver
   * channel, such as {@link com.datastax.oss.driver.api.core.connection.ConnectionInitException} or
   * {@link com.datastax.oss.driver.api.core.connection.ClosedConnectionException}.
   *
   * <p>It will also be null if you serialize and deserialize an exception.
   */
  public ExecutionInfo getExecutionInfo() {
    return executionInfo;
  }

  /** This is for internal use by the driver, a client application has no reason to call it. */
  public void setExecutionInfo(ExecutionInfo executionInfo) {
    this.executionInfo = executionInfo;
  }

  /**
   * Copy the exception.
   *
   * <p>This returns a new exception, equivalent to the original one, except that because a new
   * object is created in the current thread, the top-most element in the stacktrace of the
   * exception will refer to the current thread. The original exception may or may not be included
   * as the copy's cause, depending on whether that is deemed useful (this is left to the discretion
   * of each implementation).
   *
   * <p>This is intended for the synchronous wrapper methods of the driver, in order to produce a
   * more user-friendly stack trace (that includes the line in the user code where the driver
   * rethrew the error).
   */
  @NonNull
  public abstract DriverException copy();
}
