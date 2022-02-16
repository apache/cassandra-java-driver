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
package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.EndPoint;

public class CASWriteUnknownException extends QueryConsistencyException {

  private static final long serialVersionUID = 0;

  /**
   * This constructor should only be used internally by the driver when decoding error responses.
   */
  public CASWriteUnknownException(ConsistencyLevel consistency, int received, int required) {
    this(null, consistency, received, required);
  }

  public CASWriteUnknownException(
      EndPoint endPoint, ConsistencyLevel consistency, int received, int required) {
    super(
        endPoint,
        String.format(
            "CAS operation result is unknown - proposal was not accepted by a quorum. (%d / %d)",
            received, required),
        consistency,
        received,
        required);
  }

  private CASWriteUnknownException(
      EndPoint endPoint,
      String msg,
      Throwable cause,
      ConsistencyLevel consistency,
      int received,
      int required) {
    super(endPoint, msg, cause, consistency, received, required);
  }

  @Override
  public CASWriteUnknownException copy() {
    return new CASWriteUnknownException(
        getEndPoint(),
        getMessage(),
        this,
        getConsistencyLevel(),
        getReceivedAcknowledgements(),
        getRequiredAcknowledgements());
  }

  /**
   * Create a copy of this exception with a nicer stack trace, and including the coordinator address
   * that caused this exception to be raised.
   *
   * <p>This method is mainly intended for internal use by the driver and exists mainly because:
   *
   * <ol>
   *   <li>the original exception was decoded from a response frame and at that time, the
   *       coordinator address was not available; and
   *   <li>the newly-created exception will refer to the current thread in its stack trace, which
   *       generally yields a more user-friendly stack trace that the original one.
   * </ol>
   *
   * @param endPoint The full address of the host that caused this exception to be thrown.
   * @return a copy/clone of this exception, but with the given host address instead of the original
   *     one.
   */
  public CASWriteUnknownException copy(EndPoint endPoint) {
    return new CASWriteUnknownException(
        endPoint,
        getMessage(),
        this,
        getConsistencyLevel(),
        getReceivedAcknowledgements(),
        getRequiredAcknowledgements());
  }
}
