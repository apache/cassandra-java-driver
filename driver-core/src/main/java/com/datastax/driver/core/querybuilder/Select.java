package com.datastax.driver.core.querybuilder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.datastax.driver.core.TableMetadata;

/**
 * A built SELECT statement.
 */
public class Select extends BuiltStatement {

    private static final List<String> COUNT_ALL = Collections.singletonList("count(*)");

    private final String keyspace;
    private final String table;
    private final List<String> columnNames;
    private final Where where;
    private List<Ordering> orderings;
    private int limit = -1;
    private boolean allowFiltering;

    Select(String keyspace, String table, List<String> columnNames) {
        super();
        this.keyspace = keyspace;
        this.table = table;
        this.columnNames = columnNames;
        this.where = new Where(this);
    }

    Select(TableMetadata table, List<String> columnNames) {
        super(table);
        this.keyspace = table.getKeyspace().getName();
        this.table = table.getName();
        this.columnNames = columnNames;
        this.where = new Where(this);
    }

    protected String buildQueryString() {
        StringBuilder builder = new StringBuilder();

        builder.append("SELECT ");
        if (columnNames == null) {
            builder.append("*");
        } else {
            Utils.joinAndAppendNames(builder, ",", columnNames);
        }
        builder.append(" FROM ");
        if (keyspace != null)
            Utils.appendName(keyspace, builder).append(".");
        Utils.appendName(table, builder);

        if (!where.clauses.isEmpty()) {
            builder.append(" WHERE ");
            Utils.joinAndAppend(builder, " AND ", where.clauses);
        }

        if (orderings != null) {
            builder.append(" ORDER BY ");
            Utils.joinAndAppend(builder, ",", orderings);
        }

        if (limit > 0) {
            builder.append(" LIMIT ").append(limit);
        }

        if (allowFiltering) {
            builder.append(" ALLOW FILTERING");
        }

        return builder.toString();
    }

    /**
     * Adds a WHERE clause to this statement.
     *
     * This is a shorter/more readable version for {@code where().and(clause)}.
     *
     * @param clause the clause to add.
     * @return the where clause of this query to which more clause can be added.
     */
    public Where where(Clause clause) {
        return where.and(clause);
    }

    /**
     * Returns a Where statement for this query without adding clause.
     *
     * @return the where clause of this query to which more clause can be added.
     */
    public Where where() {
        return where;
    }

    /**
     * Adds an ORDER BY clause to this statement.
     *
     * @param orderings the orderings to define for this query.
     * @return this statement.
     *
     * @throws IllegalStateException if an ORDER BY clause has already been
     * provided.
     */
    public Select orderBy(Ordering... orderings) {
        if (this.orderings != null)
            throw new IllegalStateException("An ORDER BY clause has already been provided");

        this.orderings = Arrays.asList(orderings);
        setDirty();
        return this;
    }

    /**
     * Adds a LIMIT clause to this statement.
     *
     * @param limit the limit to set.
     * @return this statement.
     *
     * @throws IllegalArgumentException if {@code limit &gte; 0}.
     * @throws IllegalStateException if a LIMIT clause has already been
     * provided.
     */
    public Select limit(int limit) {
        if (limit <= 0)
            throw new IllegalArgumentException("Invalid LIMIT value, must be strictly positive");

        if (this.limit > 0)
            throw new IllegalStateException("A LIMIT value has already been provided");

        this.limit = limit;
        setDirty();
        return this;
    }

    /**
     * Adds an ALLOW FILTERING directive to this statement.
     *
     * @return this statement.
     */
    public Select allowFiltering() {
        allowFiltering = true;
        return this;
    }

    /**
     * The WHERE clause of a SELECT statement.
     */
    public static class Where extends BuiltStatement.ForwardingStatement<Select> {

        private final List<Clause> clauses = new ArrayList<Clause>();

        Where(Select statement) {
            super(statement);
        }

        /**
         * Adds the provided clause to this WHERE clause.
         *
         * @param clause the clause to add.
         * @return this WHERE clause.
         */
        public Where and(Clause clause)
        {
            clauses.add(clause);
            statement.maybeAddRoutingKey(clause.name(), clause.firstValue());
            setDirty();
            return this;
        }

        /**
         * Adds an ORDER BY clause to the SELECT statement this WHERE clause if
         * part of.
         *
         * @param orderings the orderings to add.
         * @return the select statement this Where clause if part of.
         *
         * @throws IllegalStateException if an ORDER BY clause has already been
         * provided.
         */
        public Select orderBy(Ordering... orderings) {
            return statement.orderBy(orderings);
        }

        /**
         * Adds a LIMIT clause to the SELECT statement this Where clause if
         * part of.
         *
         * @param limit the limit to set.
         * @return the select statement this Where clause if part of.
         *
         * @throws IllegalArgumentException if {@code limit &gte; 0}.
         * @throws IllegalStateException if a LIMIT clause has already been
         * provided.
         */
        public Select limit(int limit) {
            return statement.limit(limit);
        }
    }

    /**
     * An in-construction SELECT statement.
     */
    public static class Builder {

        protected List<String> columnNames;

        protected Builder() {}

        Builder(List<String> columnNames) {
            this.columnNames = columnNames;
        }

        /**
         * Adds the table to select from.
         *
         * @param table the name of the table to select from.
         * @return a newly built SELECT statement that selects from {@code table}.
         */
        public Select from(String table) {
            return from(null, table);
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

    /**
     * An Selection clause for an in-construction SELECT statement.
     */
    public static class Selection extends Builder {

        /**
         * Selects all columns (i.e. "SELECT *  ...")
         *
         * @return an in-build SELECT statement.
         *
         * @throws IllegalStateException if some columns had already been selected for this builder.
         */
        public Builder all() {
            if (columnNames != null)
                throw new IllegalStateException(String.format("Some columns (%s) have already been selected.", columnNames));

            return (Builder)this;
        }

        /**
         * Selects the count of all returned rows (i.e. "SELECT count(*) ...").
         *
         * @return an in-build SELECT statement.
         *
         * @throws IllegalStateException if some columns had already been selected for this builder.
         */
        public Builder countAll() {
            if (columnNames != null)
                throw new IllegalStateException(String.format("Some columns (%s) have already been selected.", columnNames));

            columnNames = COUNT_ALL;
            return (Builder)this;
        }

        /**
         * Selects the provided column.
         *
         * @param name the new column name to add.
         * @return this in-build SELECT statement
         */
        public Selection column(String name) {
            if (columnNames == null)
                columnNames = new ArrayList<String>();

            columnNames.add(name);
            return this;
        }

        /**
         * Selects the write time of provided column.
         *
         * @param name the name of the column to select the write time of.
         * @return this in-build SELECT statement
         */
        public Selection writeTime(String name) {
            StringBuilder sb = new StringBuilder();
            sb.append("writetime(");
            Utils.appendName(name, sb);
            sb.append(")");
            return column(sb.toString());
        }

        /**
         * Selects the ttl of provided column.
         *
         * @param name the name of the column to select the ttl of.
         * @return this in-build SELECT statement
         */
        public Selection ttl(String name) {
            StringBuilder sb = new StringBuilder();
            sb.append("ttl(");
            Utils.appendName(name, sb);
            sb.append(")");
            return column(sb.toString());
        }
    }
}
