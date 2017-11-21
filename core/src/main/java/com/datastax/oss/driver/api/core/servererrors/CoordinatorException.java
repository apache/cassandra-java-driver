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
package com.datastax.oss.driver.api.core.servererrors;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.metadata.Node;

/** A server-side error thrown by the coordinator node in response to a driver request. */
public abstract class CoordinatorException extends DriverException {

  private final Node coordinator;

  protected CoordinatorException(Node coordinator, String message, boolean writableStackTrace) {
    super(message, null, writableStackTrace);
    this.coordinator = coordinator;
  }

  public Node getCoordinator() {
    return coordinator;
  }
}
