package com.datastax.driver.mapping;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.*;

/**
 * A object to update some of the fields of an entity.
 */
public class Updater<T> {

    private final Mapper<T> mapper;
    private final Object[] primaryKey;

    private final List<Assignment> assignments = new ArrayList<Assignment>();

    protected BoundStatement statement;

    Updater(Mapper<T> mapper, Object[] primaryKey) {
        this.mapper = mapper;
        this.primaryKey = primaryKey;
    }

    public Updater<T> with(Assignment assignment) {
        if (statement != null)
            throw new IllegalStateException("Cannot modify the updater after getStatement() has been called");

        this.assignments.add(assignment);
        return this;
    }

    public Conditional onlyIf(Clause condition) {
        return new Conditional().and(condition);
    }

    /**
     * The statement to be used by this Updater for execution.
     * <p>
     * This method trigger the creation of the statement that the Updater will use for execution and returns
     * it. The main reason to use that method is to set a number of query options on the returned statement
     * (like tracing, some specific consistency level, ...) before calling one of {@code execute}/{@code executeAsync}.
     *
     * @return the statement to be used by this Updater for execution.
     */
    public Statement getStatement() {
        if (statement == null) {
            // We sort the assignements to make sure we don't reprepare the same query twice just because the Assignments
            // haven't been provided in the same order.
            Collections.sort(assignements, ASSIGNMENTS_COMPARATOR);
            QueryType type = QueryType.update(assignements);
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
     * Executes the update defined by this updater.
     */
    public void execute() {
        return mapper.map(mapper.session().execute(getStatement()));
    }

    /**
     * Executes the update defined by this updater asynchronously.
     *
     * @return a future on the completion of the update.
     */
    public ListenableFuture<Void> executeAsync() {
        return Futures.transform(mapper.session().executeAsync(getStatement()), mapper.NOOP);
    }

    public class Conditional {

        private final List<Clause> conditions = new ArrayList<Conditions>();

        public Conditional and(Clause condition) {
            if (statement != null)
                throw new IllegalStateException("Cannot modify the updater after getStatement() has been called");

            this.conditions.add(condition);
            return this;
        }

        /**
         * The statement to be used by this conditional updater for execution.
         * <p>
         * This method trigger the creation of the statement that the Updater will use for execution and returns
         * it. The main reason to use that method is to set a number of query options on the returned statement
         * (like tracing, some specific consistency level, ...) before calling one of {@code execute}/{@code executeAsync}.
         *
         * @return the statement to be used by this Updater for execution.
         */
        public Statement getStatement() {
        }

        /**
         * Executes the update defined by this updater.
         */
        public ConditionalUpdateResult<T> execute() {
            return mapper.map(mapper.session().execute(getStatement()));
        }

        /**
         * Executes the update defined by this updater asynchronously.
         *
         * @return a future on the completion of the update.
         */
        public ListenableFuture<ConditionalUpdateResult<T>> executeAsync() {
            return Futures.transform(mapper.session().executeAsync(getStatement()), makeConditionalResultFunction);
        }
    }

}

