package com.datastax.driver.mapping;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.google.common.util.concurrent.ListenableFuture;

public class ResultFuture<T> implements ListenableFuture<Result<T>> {
	private final ResultSetFuture futureResultSet;
	private final Mapper mapper;
	boolean fetchColumnsUsingIndex;
	
	ResultFuture(ResultSetFuture futureResultSet, Mapper mapper) {
		this.futureResultSet = futureResultSet;
		this.mapper = mapper;
	}
	
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return futureResultSet.cancel(mayInterruptIfRunning);
	}

	@Override
	public Result<T> get() throws InterruptedException, ExecutionException {
		ResultSet rs = futureResultSet.get();
		return new Result<T>(rs, mapper);
	}
	
	@Override
	public Result<T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		ResultSet rs = futureResultSet.get(timeout, unit);
		return new Result<T>(rs, mapper);
	}

    /**
     * Waits for the query to return and return its mapped entities.
     *
     * This method is usually more convenient than {@link #get} as it:
     * <ul>
     *   <li>It waits for the result uninterruptibly, and so doesn't throw
     *   {@link InterruptedException}.</li>
     *   <li>It returns meaningful exceptions, instead of having to deal
     *   with ExecutionException.</li>
     * </ul>
     * As such, it is the preferred way to get the future result.
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     */
    public Result<T> getUninterruptibly() throws NoHostAvailableException {
    	ResultSet rs = futureResultSet.getUninterruptibly();
    	return new Result<T>(rs, mapper);
    }

    /**
     * Waits for the given time for the query to return and return its
     * mapped entities if available.
     *
     * This method is usually more convenient than {@link #get} as it:
     * <ul>
     *   <li>It waits for the result uninterruptibly, and so doesn't throw
     *   {@link InterruptedException}.</li>
     *   <li>It returns meaningful exceptions, instead of having to deal
     *   with ExecutionException.</li>
     * </ul>
     * As such, it is the preferred way to get the future result.
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     * @throws TimeoutException if the wait timed out (Note that this is
     * different from a Cassandra timeout, which is a {@code
     * QueryExecutionException}).
     */
    public Result<T> getUninterruptibly(long timeout, TimeUnit unit) throws NoHostAvailableException, TimeoutException {
    	ResultSet rs = futureResultSet.getUninterruptibly(timeout, unit);
    	return new Result<T>(rs, mapper);
    }
    
	@Override
	public boolean isCancelled() {
		return futureResultSet.isCancelled();
	}

	@Override
	public boolean isDone() {
		return futureResultSet.isDone();
	}

	@Override
	public void addListener(Runnable listener, Executor executor) {
		futureResultSet.addListener(listener, executor);
	}
}