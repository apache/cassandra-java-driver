/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core.policies;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * A policy that defines a default behavior to adopt in the event of unexpected errors.
 * <p>
 * This interface exists only for backward compatibility reasons: its methods should really be
 * defined by {@link RetryPolicy}, but adding it after the fact would break binary compatibility.
 * It should be merged into {@link RetryPolicy} in the next major release.
 * <p>
 * All retry policies shipped with the driver implement this interface.
 */
public interface ExtendedRetryPolicy extends RetryPolicy {

    /**
     * Defines whether to retry and at which consistency level on an
     * unexpected error.
     * <p/>
     * This method might be invoked in the following situations:
     * <ol>
     * <li>On a client timeout, while waiting for the server response
     * (see {@link SocketOptions#getReadTimeoutMillis()});</li>
     * <li>On a connection error (socket closed, etc.);</li>
     * <li>When the contacted host replies with an error, such as
     * {@code OVERLOADED}, {@code IS_BOOTSTRAPPING}, {@code SERVER_ERROR}, etc.</li>
     * </ol>
     * <p/>
     * Note that when this method is invoked, <em>the driver cannot guarantee that the mutation has
     * been effectively applied server-side</em>; a retry should only be attempted if the request
     * is known to be idempotent.
     *
     * @param statement the original query that failed.
     * @param cl        the requested consistency level for the operation.
     *                  Note that this is not necessarily the achieved consistency level (if any),
     *                  and it is never a {@link ConsistencyLevel#isSerial() serial} one.
     * @param e         the exception that caused this request to fail.
     * @param nbRetry   the number of retries already performed for this operation.
     * @return the retry decision. If {@code RetryDecision.RETHROW} is returned,
     * the {@link DriverException} passed to this method will be thrown for the operation.
     */
    RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, Exception e, int nbRetry);

}
