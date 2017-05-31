/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
 */
public abstract class DriverException extends RuntimeException {
  protected DriverException(String message) {
    super(message);
  }

  protected DriverException(String message, Throwable cause) {
    super(message, cause);
  }

  protected DriverException(Throwable cause) {
    super(cause);
  }
}
