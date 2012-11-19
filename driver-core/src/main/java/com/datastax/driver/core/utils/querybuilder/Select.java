package com.datastax.driver.core.utils.querybuilder;

import com.datastax.driver.core.TableMetadata;

/**
 * A built SELECT statement.
 */
public class Select extends BuiltStatement {

    private boolean whereProvided;
    private boolean orderByProvided;
    private boolean limitProvided;

    Select(String keyspace, String table, String[] columnNames) {
        super();
        init(keyspace, table, columnNames);
    }

    Select(TableMetadata table, String[] columnNames) {
        super(table);
        init(table.getKeyspace().getName(), table.getName(), columnNames);
    }

    private void init(String keyspaceName, String tableName, String[] columnNames) {
        builder.append("SELECT ");
        if (columnNames.length == 0) {
            builder.append("*");
        } else {
            Utils.joinAndAppendNames(builder, ",", columnNames);
        }
        builder.append(" FROM ");
        if (keyspaceName != null)
            appendName(keyspaceName).append(".");
        appendName(tableName);
    }

    /**
     * Adds a WHERE clause to this statement.
     *
     * @param clause the clause to add.
     * @return this statement.
     *
     * @throws IllegalStateException if WHERE clauses have already been
     * provided.
     */
    public Select where(Clause clause) {
        if (whereProvided)
            throw new IllegalStateException("A WHERE clause has already been provided");

        whereProvided = true;
        builder.append(" WHERE ");

        clause.appendTo(builder);
        maybeAddRoutingKey(clause.name(), clause.firstValue());
        return this;
    }

    /**
     * Adds WHERE clauses to this statement.
     *
     * @param clauses the clauses to add.
     * @return this statement.
     *
     * @throws IllegalStateException if WHERE clauses have already been
     * provided.
     */
    public Select where(Clause... clauses) {
        if (whereProvided)
            throw new IllegalStateException("A WHERE clause has already been provided");

        whereProvided = true;
        builder.append(" WHERE ");

        Utils.joinAndAppend(this, builder, " AND ", clauses);

        for (int i = 0; i < clauses.length; ++i)
            maybeAddRoutingKey(clauses[i].name(), clauses[i].firstValue());
        return this;
    }

    /**
     * Adds an ORDER BY clause to this statement.
     *
     * @param orders the orderings to define for this query.
     * @return this statement.
     *
     * @throws IllegalStateException if an ORDER BY clause has already been
     * provided.
     */
    public Select orderBy(Ordering... orders) {
        if (orderByProvided)
            throw new IllegalStateException("An ORDER BY clause has already been provided");

        orderByProvided = true;
        builder.append(" ORDER BY (");
        Utils.joinAndAppend(null, builder, ",", orders);
        builder.append(")");
        return this;
    }

    /**
     * Adds a LIMIT clause to this statement.
     *
     * @param limit the limit to set.
     * @return this statement.
     *
     * @throws IllegalStateException if a LIMIT clause has already been
     * provided.
     */
    public Select limit(int limit) {
        if (limitProvided)
            throw new IllegalStateException("A LIMIT value has already been provided");

        limitProvided = true;
        builder.append(" LIMIT ").append(limit);
        return this;
    }

    /**
     * An in-construction SELECT statement.
     */
    public static class Builder {

        private final String[] columnNames;

        Builder(String[] columnNames) {
            this.columnNames = columnNames;
        }

        /**
         * Adds the table to select from.
         *
         * @param table the name of the table to select from.
         * @return a newly built SELECT statement that selects from {@code table}.
         */
        public Select from(String table) {
            return new Select(null, table, columnNames);
        }

        /**
         * Adds the table to select from.
         *
         * @param keyspace the name of the keyspace to select from.
         * @param table the name of the table to select from.
         * @return a newly built SELECT statement that selects from {@code keyspace.table}.
         */
        public Select from(String keyspace, String table) {
            return new Select(keyspace, table, columnNames);
        }

        /**
         * Adds the table to select from.
         *
         * @param table the table to select from.
         * @return a newly built SELECT statement that selects from {@code table}.
         */
        public Select from(TableMetadata table) {
            return new Select(table, columnNames);
        }
    }
}
