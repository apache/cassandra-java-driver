package com.datastax.driver.mapping;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.datastax.driver.core.*;

/**
 * A object to fetch a slice of entities over a given partition (of the table mapped
 * by the Mapper this Slicer has been created from).
 */
public class Slicer<T> {

    private final Mapper<T> mapper;
    private final Object[] partitionKey;

    private Object[] from;
    private Object[] to;

    private Integer limit;
    private boolean reversed;

    private BoundStatement statement;

    Slicer(Mapper<T> mapper, Object[] partitionKey) {
        this.mapper = mapper;
        this.partitionKey = partitionKey;
    }

    /**
     * Defines the starting bound of the slice to fetch.
     * <p>
     * Calling this method is optional. If no starting bound is defined,
     * the fetched slice will start at the beginning of the partition sliced.
     *
     * @param clusteringColumns the clustering columns values that define the
     * start of the partition slice to fetch. The values must be provided in
     * the order of the clustering columns in the PRIMARY KEY, but it is allowed
     * to provide only a prefix of all the clustering columns.
     * @return this Slicer.
     *
     * @throws IllegalStateException if the starting bound for this slicer has
     * already been set (i.e. if this method is called for a second time).
     * @throws IllegalArgumentException if the number of provided values is greater
     * than the number of clustering columns.
     */
    public Slicer<T> from(Object... clusteringColumns) {
        if (statement != null)
            throw new IllegalStateException("Cannot modify the slicer parameters after getStatement() has been called");

        if (from != null)
            throw new IllegalStateException("The 'from' has already been set on this Slicer");

        if (clusteringColumns.length > mapper.mapper.clusteringColumns.size())
            throw new IllegalArgumentException(String.format("Too many clustering column provided; expecting a maximum of %d but got %d",
                        mapper.mapper.clusteringColumns.size(), clusteringColumns.length));

        this.from = clusteringColumns;
        return this;
    }

    /**
     * Defines the ending bound of the slice to fetch.
     * <p>
     * Calling this method is optional. If no ending bound is defined,
     * the fetched slice will extend to end of the partition sliced.
     *
     * @param clusteringColumns the clustering columns values that define the
     * end of the partition slice to fetch. The values must be provided in
     * the order of the clustering columns in the PRIMARY KEY, but it is allowed
     * to provide only a prefix of all the clustering columns.
     * @return this Slicer.
     *
     * @throws IllegalStateException if the ending bound for this slicer has
     * already been set (i.e. if this method is called for a second time).
     * @throws IllegalArgumentException if the number of provided values is greater
     * than the number of clustering columns.
     */
    public Slicer<T> to(Object... clusteringColumns) {
        if (statement != null)
            throw new IllegalStateException("Cannot modify the slicer parameters after getStatement() has been called");
        if (to != null)
            throw new IllegalStateException("The 'to' has already been set on this Slicer");

        if (clusteringColumns.length > mapper.mapper.clusteringColumns.size())
            throw new IllegalArgumentException(String.format("Too many clustering column provided; expecting a maximum of %d but got %d",
                        mapper.mapper.clusteringColumns.size(), clusteringColumns.length));

        this.to = clusteringColumns;
        return this;
    }

    /**
     * Defines a limit on the number of entities to be fetched by this slicer.
     * <p>
     * By default (if this method isn't called), no limit is enforced.
     *
     * @param limit the limit to set for this slicer.
     * @return this Slicer.
     */
    public Slicer<T> limit(int limit) {
        if (statement != null)
            throw new IllegalStateException("Cannot modify the slicer parameters after getStatement() has been called");
        if (this.limit != null)
            throw new IllegalStateException("The 'limit' has already been set on this Slicer");
        if (limit <= 0)
            throw new IllegalArgumentException("Invalid limit, must be strictly positive");

        this.limit = limit;
        return this;
    }

    /**
     * Reverse the order of the entities fetched in the result.
     * <p>
     * Note that if a limit l is set on this slicer and the slicer is reversed,
     * then only the l last entities of the slice will be returned (and the last
     * entity of the slice will be returned first, etc...).
     *
     * @return this Slicer.
     */
    public Slicer<T> reversed() {
        if (statement != null)
            throw new IllegalStateException("Cannot modify the slicer parameters after getStatement() has been called");
        this.reversed = true;
        return this;
    }

    /**
     * The statement to be used by this Slicer for execution.
     * <p>
     * This method trigger the creation of the statement that the Slicer will use for execution and returns
     * it. The main reason to use that method is to set a number of query options on the returned statement
     * (like tracing, some specific consistency level, ...) before calling one of {@code execute}/{@code executeAsync}.
     *
     * @return the statement to be used by this Slicer for execution.
     */
    public Statement getStatement() {
        if (statement == null) {
            QueryType type = QueryType.slice(from == null ? 0 : from.length, true, to == null ? 0 : to.length, true, reversed);
            PreparedStatement ps = mapper.getPreparedQuery(type);

            statement = ps.bind();

            int idx = 0;
            for (int i = 0; i < mapper.mapper.partitionKeys.size(); i++)
                statement.setBytesUnsafe(idx++, mapper.mapper.partitionKeys.get(i).getDataType().serialize(partitionKey[i]));

            if (from != null && from.length > 0) {
                for (int i = 0; i < from.length; i++)
                    statement.setBytesUnsafe(idx++, mapper.mapper.clusteringColumns.get(i).getDataType().serialize(from[i]));
            }

            if (to != null && to.length > 0) {
                for (int i = 0; i < to.length; i++)
                    statement.setBytesUnsafe(idx++, mapper.mapper.clusteringColumns.get(i).getDataType().serialize(to[i]));
            }

            statement.setInt(idx++, limit == null ? Integer.MAX_VALUE : limit);

            if (mapper.mapper.readConsistency != null)
                statement.setConsistencyLevel(mapper.mapper.readConsistency);
        }
        return statement;
    }

    /**
     * Fetches the slice of entities defined by this Slicer.
     *
     * @return the entities fetched by this slicer.
     */
    public Result<T> execute() {
        return mapper.map(mapper.session().execute(getStatement()));
    }

    /**
     * Fetches the slice of entities defined by this Slicer asynchronously.
     *
     * @return a future on the entities fetched by this slicer.
     */
    public ListenableFuture<Result<T>> executeAsync() {
        return Futures.transform(mapper.session().executeAsync(getStatement()), mapper.mapAllFunction);
    }
}
