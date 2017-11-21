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
 * Thrown by synchronous wrapper methods (such as {@link Cluster#connect()}, when the underlying
 * future was completed with a <em>checked</em> exception.
 *
 * <p>This exception should be rarely thrown (if ever). Most of the time, the driver uses unchecked
 * exceptions, which will be rethrown directly instead of being wrapped in this class.
 */
public class DriverExecutionException extends DriverException {
  public DriverExecutionException(Throwable cause) {
    super(null, cause, true);
  }

  @Override
  public DriverException copy() {
    return new DriverExecutionException(getCause());
  }
}
