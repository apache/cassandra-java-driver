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

    private final Assignments assignments = new Assignments();
    private Conditions conditions = null;

    private BoundStatement statement;

    Updater(Mapper<T> mapper, Object[] primaryKey) {
        this.mapper = mapper;
        this.primaryKey = primaryKey;
    }

    public class Assignments {
        private final List<Assignment> assignments = new ArrayList<Assignment>();

        public Assignments and(Assignment assignment) {
            this.assignments.add(assignment);
            return this;
        }

        public Conditions onlyIf(Clause clause) {
            return Updater.this.onlyIf(clause);
        }

        public Statement getStatement() {
            return Updater.this.getStatement();
        }

        public Statement getStatement() {
            return Updater.this.getStatement();
        }
    }

    public class Conditions {
        private final List<Clause> conditions = new ArrayList<Conditions>();

        public Conditions and(Clause condition) {
            this.conditions.add(condition);
            return this;
        }

        public Statement getStatement() {
            return Updater.this.getStatement();
        }
    }

    public Assignments with(Assignment assignment) {
        return assignments.and(assignment);
    }

    public Conditions onlyIf(Clause condition) {
        if (conditions == null)
            conditions = new Conditions();
        return conditions.and(condition);
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
