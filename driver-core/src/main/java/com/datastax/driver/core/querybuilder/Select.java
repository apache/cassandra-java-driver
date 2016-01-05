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
package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TableMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A built SELECT statement.
 */
public class Select extends BuiltStatement {

    private static final List<Object> COUNT_ALL = Collections.<Object>singletonList(new Utils.FCall("count", new Utils.RawString("*")));

    private final String table;
    private final boolean isDistinct;
    private final List<Object> columnNames;
    private final Where where;
    private List<Ordering> orderings;
    private Object limit;
    private boolean allowFiltering;

    Select(String keyspace, String table, List<Object> columnNames, boolean isDistinct) {
        super(keyspace);
        this.table = table;
        this.isDistinct = isDistinct;
        this.columnNames = columnNames;
        this.where = new Where(this);
    }

    Select(TableMetadata table, List<Object> columnNames, boolean isDistinct) {
        super(table);
        this.table = escapeId(table.getName());
        this.isDistinct = isDistinct;
        this.columnNames = columnNames;
        this.where = new Where(this);
    }

    @Override
    StringBuilder buildQueryString(List<Object> variables, CodecRegistry codecRegistry) {
        StringBuilder builder = new StringBuilder();

        builder.append("SELECT ");
        if (isDistinct)
            builder.append("DISTINCT ");
        if (columnNames == null) {
            builder.append('*');
        } else {
            Utils.joinAndAppendNames(builder, codecRegistry, ",", columnNames);
        }
        builder.append(" FROM ");
        if (keyspace != null)
            Utils.appendName(keyspace, builder).append('.');
        Utils.appendName(table, builder);

        if (!where.clauses.isEmpty()) {
            builder.append(" WHERE ");
            Utils.joinAndAppend(builder, codecRegistry, " AND ", where.clauses, variables);
        }

        if (orderings != null) {
            builder.append(" ORDER BY ");
            Utils.joinAndAppend(builder, codecRegistry, ",", orderings, variables);
        }

        if (limit != null) {
            builder.append(" LIMIT ").append(limit);
        }

        if (allowFiltering) {
            builder.append(" ALLOW FILTERING");
        }

        return builder;
    }

    /**
     * Adds a WHERE clause to this statement.
     * <p/>
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
     * @throws IllegalStateException if an ORDER BY clause has already been
     *                               provided.
     */
    public Select orderBy(Ordering... orderings) {
        if (this.orderings != null)
            throw new IllegalStateException("An ORDER BY clause has already been provided");

        this.orderings = Arrays.asList(orderings);
        for (int i = 0; i < orderings.length; i++)
            checkForBindMarkers(orderings[i]);
        return this;
    }

    /**
     * Adds a LIMIT clause to this statement.
     *
     * @param limit the limit to set.
     * @return this statement.
     * @throws IllegalArgumentException if {@code limit &gte; 0}.
     * @throws IllegalStateException    if a LIMIT clause has already been
     *                                  provided.
     */
    public Select limit(int limit) {
        if (limit <= 0)
            throw new IllegalArgumentException("Invalid LIMIT value, must be strictly positive");

        if (this.limit != null)
            throw new IllegalStateException("A LIMIT value has already been provided");

        this.limit = limit;
        checkForBindMarkers(null);
        return this;
    }

    /**
     * Adds a prepared LIMIT clause to this statement.
     *
     * @param marker the marker to use for the limit.
     * @return this statement.
     * @throws IllegalStateException if a LIMIT clause has already been
     *                               provided.
     */
    public Select limit(BindMarker marker) {
        if (this.limit != null)
            throw new IllegalStateException("A LIMIT value has already been provided");

        this.limit = marker;
        checkForBindMarkers(marker);
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
        public Where and(Clause clause) {
            clauses.add(clause);
            statement.maybeAddRoutingKey(clause.name(), clause.firstValue());
            checkForBindMarkers(clause);
            return this;
        }

        /**
         * Adds an ORDER BY clause to the SELECT statement this WHERE clause if
         * part of.
         *
         * @param orderings the orderings to add.
         * @return the select statement this Where clause if part of.
         * @throws IllegalStateException if an ORDER BY clause has already been
         *                               provided.
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
         * @throws IllegalArgumentException if {@code limit &gte; 0}.
         * @throws IllegalStateException    if a LIMIT clause has already been
         *                                  provided.
         */
        public Select limit(int limit) {
            return statement.limit(limit);
        }

        /**
         * Adds a bind marker for the LIMIT clause to the SELECT statement this
         * Where clause if part of.
         *
         * @param limit the bind marker to use as limit.
         * @return the select statement this Where clause if part of.
         * @throws IllegalStateException if a LIMIT clause has already been
         *                               provided.
         */
        public Select limit(BindMarker limit) {
            return statement.limit(limit);
        }
    }

    /**
     * An in-construction SELECT statement.
     */
    public static class Builder {

        List<Object> columnNames;
        boolean isDistinct;

        Builder() {
        }

        Builder(List<Object> columnNames) {
            this.columnNames = columnNames;
        }

        /**
         * Uses DISTINCT selection.
         *
         * @return this in-build SELECT statement.
         */
        public Builder distinct() {
            this.isDistinct = true;
            return this;
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
         * @param table    the name of the table to select from.
         * @return a newly built SELECT statement that selects from {@code keyspace.table}.
         */
        public Select from(String keyspace, String table) {
            return new Select(keyspace, table, columnNames, isDistinct);
        }

        /**
         * Adds the table to select from.
         *
         * @param table the table to select from.
         * @return a newly built SELECT statement that selects from {@code table}.
         */
        public Select from(TableMetadata table) {
            return new Select(table, columnNames, isDistinct);
        }
    }

    /**
     * An Selection clause for an in-construction SELECT statement.
     */
    public static abstract class Selection extends Builder {

        /**
         * Uses DISTINCT selection.
         *
         * @return this in-build SELECT statement.
         */
        @Override
        public Selection distinct() {
            this.isDistinct = true;
            return this;
        }

        /**
         * Selects all columns (i.e. "SELECT *  ...")
         *
         * @return an in-build SELECT statement.
         * @throws IllegalStateException if some columns had already been selected for this builder.
         */
        public abstract Builder all();

        /**
         * Selects the count of all returned rows (i.e. "SELECT count(*) ...").
         *
         * @return an in-build SELECT statement.
         * @throws IllegalStateException if some columns had already been selected for this builder.
         */
        public abstract Builder countAll();

        /**
         * Selects the provided column.
         *
         * @param name the new column name to add.
         * @return this in-build SELECT statement
         */
        public abstract SelectionOrAlias column(String name);

        /**
         * Selects the write time of provided column.
         * <p/>
         * This is a shortcut for {@code fcall("writetime", QueryBuilder.column(name))}.
         *
         * @param name the name of the column to select the write time of.
         * @return this in-build SELECT statement
         */
        public abstract SelectionOrAlias writeTime(String name);

        /**
         * Selects the ttl of provided column.
         * <p/>
         * This is a shortcut for {@code fcall("ttl", QueryBuilder.column(name))}.
         *
         * @param name the name of the column to select the ttl of.
         * @return this in-build SELECT statement
         */
        public abstract SelectionOrAlias ttl(String name);

        /**
         * Creates a function call.
         * <p/>
         * Please note that the parameters are interpreted as values, and so
         * {@code fcall("textToBlob", "foo")} will generate the string
         * {@code "textToBlob('foo')"}. If you want to generate
         * {@code "textToBlob(foo)"}, i.e. if the argument must be interpreted
         * as a column name (in a select clause), you will need to use the
         * {@link QueryBuilder#column} method, and so
         * {@code fcall("textToBlob", QueryBuilder.column(foo)}.
         *
         * @param name       the name of the function.
         * @param parameters the parameters for the function call.
         * @return this in-build SELECT statement
         */
        public abstract SelectionOrAlias fcall(String name, Object... parameters);
    }

    /**
     * An Selection clause for an in-construction SELECT statement.
     * <p/>
     * This only differs from {@link Selection} in that you can add an
     * alias for the previously selected item through {@link SelectionOrAlias#as}.
     */
    public static class SelectionOrAlias extends Selection {

        private Object previousSelection;

        /**
         * Adds an alias for the just selected item.
         *
         * @param alias the name of the alias to use.
         * @return this in-build SELECT statement
         */
        public Selection as(String alias) {
            assert previousSelection != null;
            Object a = new Utils.Alias(previousSelection, alias);
            previousSelection = null;
            return addName(a);
        }

        // We don't return SelectionOrAlias on purpose
        private Selection addName(Object name) {
            if (columnNames == null)
                columnNames = new ArrayList<Object>();

            columnNames.add(name);
            return this;
        }

        private SelectionOrAlias queueName(Object name) {
            if (previousSelection != null)
                addName(previousSelection);

            previousSelection = name;
            return this;
        }

        /**
         * Selects all columns (i.e. "SELECT *  ...")
         *
         * @return an in-build SELECT statement.
         * @throws IllegalStateException if some columns had already been selected for this builder.
         */
        @Override
        public Builder all() {
            if (columnNames != null)
                throw new IllegalStateException(String.format("Some columns (%s) have already been selected.", columnNames));
            if (previousSelection != null)
                throw new IllegalStateException(String.format("Some columns ([%s]) have already been selected.", previousSelection));

            return (Builder) this;
        }

        /**
         * Selects the count of all returned rows (i.e. "SELECT count(*) ...").
         *
         * @return an in-build SELECT statement.
         * @throws IllegalStateException if some columns had already been selected for this builder.
         */
        @Override
        public Builder countAll() {
            if (columnNames != null)
                throw new IllegalStateException(String.format("Some columns (%s) have already been selected.", columnNames));
            if (previousSelection != null)
                throw new IllegalStateException(String.format("Some columns ([%s]) have already been selected.", previousSelection));

            columnNames = COUNT_ALL;
            return (Builder) this;
        }

        /**
         * Selects the provided column.
         *
         * @param name the new column name to add.
         * @return this in-build SELECT statement
         */
        @Override
        public SelectionOrAlias column(String name) {
            return queueName(name);
        }

        /**
         * Selects the provided raw expression.
         * <p/>
         * This method is used internally by the mapper module. It is not exposed on the parent class {@link Selection} to
         * avoid breaking binary compatibility. This means that it is not accessible directly via the fluent API (you need
         * a cast). This shouldn't be a problem for regular clients because they will use other methods of the DSL
         * ({@code fcall}, etc.) rather than provide a raw string.
         *
         * @param rawString the raw expression to add.
         * @return this in-build SELECT statement
         */
        public SelectionOrAlias raw(String rawString) {
            return queueName(QueryBuilder.raw(rawString));
        }

        /**
         * Selects the write time of provided column.
         * <p/>
         * This is a shortcut for {@code fcall("writetime", QueryBuilder.column(name))}.
         *
         * @param name the name of the column to select the write time of.
         * @return this in-build SELECT statement
         */
        @Override
        public SelectionOrAlias writeTime(String name) {
            return queueName(new Utils.FCall("writetime", new Utils.CName(name)));
        }

        /**
         * Selects the ttl of provided column.
         * <p/>
         * This is a shortcut for {@code fcall("ttl", QueryBuilder.column(name))}.
         *
         * @param name the name of the column to select the ttl of.
         * @return this in-build SELECT statement
         */
        @Override
        public SelectionOrAlias ttl(String name) {
            return queueName(new Utils.FCall("ttl", new Utils.CName(name)));
        }

        /**
         * Creates a function call.
         * <p/>
         * Please note that the parameters are interpreted as values, and so
         * {@code fcall("textToBlob", "foo")} will generate the string
         * {@code "textToBlob('foo')"}. If you want to generate
         * {@code "textToBlob(foo)"}, i.e. if the argument must be interpreted
         * as a column name (in a select clause), you will need to use the
         * {@link QueryBuilder#column} method, and so
         * {@code fcall("textToBlob", QueryBuilder.column(foo)}.
         */
        @Override
        public SelectionOrAlias fcall(String name, Object... parameters) {
            return queueName(new Utils.FCall(name, parameters));
        }

        /**
         * Adds the table to select from.
         *
         * @param keyspace the name of the keyspace to select from.
         * @param table    the name of the table to select from.
         * @return a newly built SELECT statement that selects from {@code keyspace.table}.
         */
        @Override
        public Select from(String keyspace, String table) {
            if (previousSelection != null)
                addName(previousSelection);
            return super.from(keyspace, table);
        }

        /**
         * Adds the table to select from.
         *
         * @param table the table to select from.
         * @return a newly built SELECT statement that selects from {@code table}.
         */
        @Override
        public Select from(TableMetadata table) {
            if (previousSelection != null)
                addName(previousSelection);
            return super.from(table);
        }
    }
}
