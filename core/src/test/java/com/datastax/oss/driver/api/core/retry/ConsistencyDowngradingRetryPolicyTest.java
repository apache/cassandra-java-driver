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
package com.datastax.oss.driver.api.core.retry;

import static com.datastax.oss.driver.api.core.ConsistencyLevel.EACH_QUORUM;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.ONE;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.QUORUM;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.SERIAL;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.THREE;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.TWO;
import static com.datastax.oss.driver.api.core.retry.RetryDecision.IGNORE;
import static com.datastax.oss.driver.api.core.retry.RetryDecision.RETHROW;
import static com.datastax.oss.driver.api.core.retry.RetryDecision.RETRY_NEXT;
import static com.datastax.oss.driver.api.core.retry.RetryDecision.RETRY_SAME;
import static com.datastax.oss.driver.api.core.servererrors.WriteType.BATCH;
import static com.datastax.oss.driver.api.core.servererrors.WriteType.BATCH_LOG;
import static com.datastax.oss.driver.api.core.servererrors.WriteType.CAS;
import static com.datastax.oss.driver.api.core.servererrors.WriteType.CDC;
import static com.datastax.oss.driver.api.core.servererrors.WriteType.COUNTER;
import static com.datastax.oss.driver.api.core.servererrors.WriteType.SIMPLE;
import static com.datastax.oss.driver.api.core.servererrors.WriteType.UNLOGGED_BATCH;
import static com.datastax.oss.driver.api.core.servererrors.WriteType.VIEW;

import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.servererrors.TruncateException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.internal.core.retry.ConsistencyDowngradingRetryPolicy;
import org.junit.Test;

public class ConsistencyDowngradingRetryPolicyTest extends RetryPolicyTestBase {

  public ConsistencyDowngradingRetryPolicyTest() {
    super(new ConsistencyDowngradingRetryPolicy("test"));
  }

  @Test
  public void should_process_read_timeouts() {
    // retry count != 0
    assertOnReadTimeout(QUORUM, 2, 2, false, 1).hasDecision(RETHROW);
    // serial CL
    assertOnReadTimeout(SERIAL, 2, 2, false, 0).hasDecision(RETHROW);
    // received < blockFor
    assertOnReadTimeout(QUORUM, 4, 3, true, 0).hasDecision(RETRY_SAME).hasConsistency(THREE);
    assertOnReadTimeout(QUORUM, 4, 3, false, 0).hasDecision(RETRY_SAME).hasConsistency(THREE);
    assertOnReadTimeout(QUORUM, 3, 2, true, 0).hasDecision(RETRY_SAME).hasConsistency(TWO);
    assertOnReadTimeout(QUORUM, 3, 2, false, 0).hasDecision(RETRY_SAME).hasConsistency(TWO);
    assertOnReadTimeout(QUORUM, 2, 1, true, 0).hasDecision(RETRY_SAME).hasConsistency(ONE);
    assertOnReadTimeout(QUORUM, 2, 1, false, 0).hasDecision(RETRY_SAME).hasConsistency(ONE);
    assertOnReadTimeout(EACH_QUORUM, 2, 0, true, 0).hasDecision(RETRY_SAME).hasConsistency(ONE);
    assertOnReadTimeout(EACH_QUORUM, 2, 0, false, 0).hasDecision(RETRY_SAME).hasConsistency(ONE);
    assertOnReadTimeout(QUORUM, 2, 0, true, 0).hasDecision(RETHROW);
    assertOnReadTimeout(QUORUM, 2, 0, false, 0).hasDecision(RETHROW);
    // data present
    assertOnReadTimeout(QUORUM, 2, 2, false, 0).hasDecision(RETRY_SAME);
    assertOnReadTimeout(QUORUM, 2, 2, true, 0).hasDecision(RETHROW);
  }

  @Test
  public void should_process_write_timeouts() {
    // retry count != 0
    assertOnWriteTimeout(QUORUM, BATCH_LOG, 2, 0, 1).hasDecision(RETHROW);
    // SIMPLE
    assertOnWriteTimeout(QUORUM, SIMPLE, 2, 1, 0).hasDecision(IGNORE);
    assertOnWriteTimeout(QUORUM, SIMPLE, 2, 0, 0).hasDecision(RETHROW);
    // BATCH
    assertOnWriteTimeout(QUORUM, BATCH, 2, 1, 0).hasDecision(IGNORE);
    assertOnWriteTimeout(QUORUM, BATCH, 2, 0, 0).hasDecision(RETHROW);
    // UNLOGGED_BATCH
    assertOnWriteTimeout(QUORUM, UNLOGGED_BATCH, 4, 3, 0)
        .hasDecision(RETRY_SAME)
        .hasConsistency(THREE);
    assertOnWriteTimeout(QUORUM, UNLOGGED_BATCH, 3, 2, 0)
        .hasDecision(RETRY_SAME)
        .hasConsistency(TWO);
    assertOnWriteTimeout(QUORUM, UNLOGGED_BATCH, 2, 1, 0)
        .hasDecision(RETRY_SAME)
        .hasConsistency(ONE);
    assertOnWriteTimeout(EACH_QUORUM, UNLOGGED_BATCH, 2, 0, 0)
        .hasDecision(RETRY_SAME)
        .hasConsistency(ONE);
    assertOnWriteTimeout(QUORUM, UNLOGGED_BATCH, 2, 0, 0).hasDecision(RETHROW);
    // BATCH_LOG
    assertOnWriteTimeout(QUORUM, BATCH_LOG, 2, 1, 0).hasDecision(RETRY_SAME);
    // others
    assertOnWriteTimeout(QUORUM, COUNTER, 2, 1, 0).hasDecision(RETHROW);
    assertOnWriteTimeout(QUORUM, CAS, 2, 1, 0).hasDecision(RETHROW);
    assertOnWriteTimeout(QUORUM, VIEW, 2, 1, 0).hasDecision(RETHROW);
    assertOnWriteTimeout(QUORUM, CDC, 2, 1, 0).hasDecision(RETHROW);
  }

  @Test
  public void should_process_unavailable() {
    // retry count != 0
    assertOnUnavailable(QUORUM, 2, 1, 1).hasDecision(RETHROW);
    // SERIAL
    assertOnUnavailable(SERIAL, 2, 1, 0).hasDecision(RETRY_NEXT);
    // downgrade
    assertOnUnavailable(QUORUM, 4, 3, 0).hasDecision(RETRY_SAME).hasConsistency(THREE);
    assertOnUnavailable(QUORUM, 3, 2, 0).hasDecision(RETRY_SAME).hasConsistency(TWO);
    assertOnUnavailable(QUORUM, 2, 1, 0).hasDecision(RETRY_SAME).hasConsistency(ONE);
    assertOnUnavailable(EACH_QUORUM, 2, 0, 0).hasDecision(RETRY_SAME).hasConsistency(ONE);
    assertOnUnavailable(QUORUM, 2, 0, 0).hasDecision(RETHROW);
  }

  @Test
  public void should_process_aborted_request() {
    assertOnRequestAborted(ClosedConnectionException.class, 0).hasDecision(RETRY_NEXT);
    assertOnRequestAborted(ClosedConnectionException.class, 1).hasDecision(RETRY_NEXT);
    assertOnRequestAborted(HeartbeatException.class, 0).hasDecision(RETRY_NEXT);
    assertOnRequestAborted(HeartbeatException.class, 1).hasDecision(RETRY_NEXT);
    assertOnRequestAborted(Throwable.class, 0).hasDecision(RETHROW);
  }

  @Test
  public void should_process_error_response() {
    assertOnErrorResponse(ReadFailureException.class, 0).hasDecision(RETHROW);
    assertOnErrorResponse(ReadFailureException.class, 1).hasDecision(RETHROW);
    assertOnErrorResponse(WriteFailureException.class, 0).hasDecision(RETHROW);
    assertOnErrorResponse(WriteFailureException.class, 1).hasDecision(RETHROW);
    assertOnErrorResponse(WriteFailureException.class, 1).hasDecision(RETHROW);

    assertOnErrorResponse(OverloadedException.class, 0).hasDecision(RETRY_NEXT);
    assertOnErrorResponse(OverloadedException.class, 1).hasDecision(RETRY_NEXT);
    assertOnErrorResponse(ServerError.class, 0).hasDecision(RETRY_NEXT);
    assertOnErrorResponse(ServerError.class, 1).hasDecision(RETRY_NEXT);
    assertOnErrorResponse(TruncateException.class, 0).hasDecision(RETRY_NEXT);
    assertOnErrorResponse(TruncateException.class, 1).hasDecision(RETRY_NEXT);
  }
}
