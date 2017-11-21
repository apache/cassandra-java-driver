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

/**
 * Base class for all exceptions thrown by the driver.
 *
 * <p>Note that, for obvious programming errors (for example, calling {@link Cluster#connect()} on a
 * cluster instance that was previously closed), the driver might throw JDK runtime exceptions, such
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
  protected DriverException(String message, Throwable cause, boolean writableStackTrace) {
    super(message, cause, true, writableStackTrace);
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
  public abstract DriverException copy();
}
