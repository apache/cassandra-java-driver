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
package com.datastax.oss.driver.api.core.retry;

import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.servererrors.TruncateException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import org.junit.Test;

import static com.datastax.oss.driver.api.core.ConsistencyLevel.QUORUM;
import static com.datastax.oss.driver.api.core.retry.RetryDecision.RETHROW;
import static com.datastax.oss.driver.api.core.retry.RetryDecision.RETRY_NEXT;
import static com.datastax.oss.driver.api.core.retry.RetryDecision.RETRY_SAME;
import static com.datastax.oss.driver.api.core.retry.WriteType.BATCH_LOG;
import static com.datastax.oss.driver.api.core.retry.WriteType.SIMPLE;

public class DefaultRetryPolicyTest extends RetryPolicyTestBase {

  public DefaultRetryPolicyTest() {
    super(new DefaultRetryPolicy(null));
  }

  @Test
  public void should_process_read_timeouts() {
    assertOnReadTimeout(QUORUM, 2, 2, false, 0).isEqualTo(RETRY_SAME);
    assertOnReadTimeout(QUORUM, 2, 2, false, 1).isEqualTo(RETHROW);
    assertOnReadTimeout(QUORUM, 2, 2, true, 0).isEqualTo(RETHROW);
    assertOnReadTimeout(QUORUM, 2, 1, true, 0).isEqualTo(RETHROW);
    assertOnReadTimeout(QUORUM, 2, 1, false, 0).isEqualTo(RETHROW);
  }

  @Test
  public void should_process_write_timeouts() {
    assertOnWriteTimeout(QUORUM, BATCH_LOG, 2, 0, 0).isEqualTo(RETRY_SAME);
    assertOnWriteTimeout(QUORUM, BATCH_LOG, 2, 0, 1).isEqualTo(RETHROW);
    assertOnWriteTimeout(QUORUM, SIMPLE, 2, 0, 0).isEqualTo(RETHROW);
  }

  @Test
  public void should_process_unavailable() {
    assertOnUnavailable(QUORUM, 2, 1, 0).isEqualTo(RETRY_NEXT);
    assertOnUnavailable(QUORUM, 2, 1, 1).isEqualTo(RETHROW);
  }

  @Test
  public void should_process_aborted_request() {
    assertOnRequestAborted(ClosedConnectionException.class, 0).isEqualTo(RETRY_NEXT);
    assertOnRequestAborted(ClosedConnectionException.class, 1).isEqualTo(RETRY_NEXT);
    assertOnRequestAborted(HeartbeatException.class, 0).isEqualTo(RETRY_NEXT);
    assertOnRequestAborted(HeartbeatException.class, 1).isEqualTo(RETRY_NEXT);
    assertOnRequestAborted(Throwable.class, 0).isEqualTo(RETHROW);
  }

  @Test
  public void should_process_error_response() {
    assertOnErrorResponse(ReadFailureException.class, 0).isEqualTo(RETHROW);
    assertOnErrorResponse(ReadFailureException.class, 1).isEqualTo(RETHROW);
    assertOnErrorResponse(WriteFailureException.class, 0).isEqualTo(RETHROW);
    assertOnErrorResponse(WriteFailureException.class, 1).isEqualTo(RETHROW);
    assertOnErrorResponse(WriteFailureException.class, 1).isEqualTo(RETHROW);

    assertOnErrorResponse(OverloadedException.class, 0).isEqualTo(RETRY_NEXT);
    assertOnErrorResponse(OverloadedException.class, 1).isEqualTo(RETRY_NEXT);
    assertOnErrorResponse(ServerError.class, 0).isEqualTo(RETRY_NEXT);
    assertOnErrorResponse(ServerError.class, 1).isEqualTo(RETRY_NEXT);
    assertOnErrorResponse(TruncateException.class, 0).isEqualTo(RETRY_NEXT);
    assertOnErrorResponse(TruncateException.class, 1).isEqualTo(RETRY_NEXT);
  }
}
