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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A future on a {@link ResultSet}.
 * <p/>
 * Note that this class implements <a href="http://code.google.com/p/guava-libraries/">Guava</a>'s {@code
 * ListenableFuture} and can so be used with Guava's future utilities.
 */
public interface ResultSetFuture extends ListenableFuture<ResultSet> {

    /**
     * Waits for the query to return and return its result.
     * <p/>
     * This method is usually more convenient than {@link #get} because it:
     * <ul>
     * <li>Waits for the result uninterruptibly, and so doesn't throw
     * {@link InterruptedException}.</li>
     * <li>Returns meaningful exceptions, instead of having to deal
     * with ExecutionException.</li>
     * </ul>
     * As such, it is the preferred way to get the future result.
     *
     * @return the query result set.
     * @throws NoHostAvailableException if no host in the cluster can be
     *                                  contacted successfully to execute this query.
     * @throws QueryExecutionException  if the query triggered an execution
     *                                  exception, that is an exception thrown by Cassandra when it cannot execute
     *                                  the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query is invalid (syntax error,
     *                                  unauthorized or any other validation problem).
     */
    public ResultSet getUninterruptibly();

    /**
     * Waits for the provided time for the query to return and return its
     * result if available.
     * <p/>
     * This method is usually more convenient than {@link #get} because it:
     * <ul>
     * <li>Waits for the result uninterruptibly, and so doesn't throw
     * {@link InterruptedException}.</li>
     * <li>Returns meaningful exceptions, instead of having to deal
     * with ExecutionException.</li>
     * </ul>
     * As such, it is the preferred way to get the future result.
     *
     * @param timeout the time to wait for the query to return.
     * @param unit    the unit for {@code timeout}.
     * @return the query result set.
     * @throws NoHostAvailableException if no host in the cluster can be
     *                                  contacted successfully to execute this query.
     * @throws QueryExecutionException  if the query triggered an execution
     *                                  exception, that is an exception thrown by Cassandra when it cannot execute
     *                                  the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     *                                  unauthorized or any other validation problem).
     * @throws TimeoutException         if the wait timed out (Note that this is
     *                                  different from a Cassandra timeout, which is a {@code
     *                                  QueryExecutionException}).
     */
    public ResultSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException;

    /**
     * Attempts to cancel the execution of the request corresponding to this
     * future. This attempt will fail if the request has already returned.
     * <p/>
     * Please note that this only cancel the request driver side, but nothing
     * is done to interrupt the execution of the request Cassandra side (and that even
     * if {@code mayInterruptIfRunning} is true) since  Cassandra does not
     * support such interruption.
     * <p/>
     * This method can be used to ensure no more work is performed driver side
     * (which, while it doesn't include stopping a request already submitted
     * to a Cassandra node, may include not retrying another Cassandra host on
     * failure/timeout) if the ResultSet is not going to be retried. Typically,
     * the code to wait for a request result for a maximum of 1 second could
     * look like:
     * <pre>
     *   ResultSetFuture future = session.executeAsync(...some query...);
     *   try {
     *       ResultSet result = future.get(1, TimeUnit.SECONDS);
     *       ... process result ...
     *   } catch (TimeoutException e) {
     *       future.cancel(true); // Ensure any resource used by this query driver
     *                            // side is released immediately
     *       ... handle timeout ...
     *   }
     * </pre>
     *
     * @param mayInterruptIfRunning the value of this parameter is currently
     *                              ignored.
     * @return {@code false} if the future could not be cancelled (it has already
     * completed normally); {@code true} otherwise.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning);
}
