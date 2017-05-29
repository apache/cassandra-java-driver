/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.*;

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
    private final boolean isJson;
    private final List<Object> columnNames;
    private final Where where;
    private List<Ordering> orderings;
    private Object limit;
    private Object perPartitionLimit;
    private boolean allowFiltering;

    Select(String keyspace, String table, List<Object> columnNames, boolean isDistinct, boolean isJson) {
        this(keyspace, table, null, null, columnNames, isDistinct, isJson);
    }

    Select(TableMetadata table, List<Object> columnNames, boolean isDistinct, boolean isJson) {
        this(Metadata.quoteIfNecessary(table.getKeyspace().getName()),
                Metadata.quoteIfNecessary(table.getName()),
                Arrays.asList(new Object[table.getPartitionKey().size()]),
                table.getPartitionKey(),
                columnNames,
                isDistinct,
                isJson);
    }

    Select(String keyspace,
           String table,
           List<Object> routingKeyValues,
           List<ColumnMetadata> partitionKey,
           List<Object> columnNames,
           boolean isDistinct,
           boolean isJson) {
        super(keyspace, partitionKey, routingKeyValues);
        this.table = table;
        this.columnNames = columnNames;
        this.isDistinct = isDistinct;
        this.isJson = isJson;
        this.where = new Where(this);
    }

    @Override
    StringBuilder buildQueryString(List<Object> variables, CodecRegistry codecRegistry) {
        StringBuilder builder = new StringBuilder();

        builder.append("SELECT ");

        if (isJson)
            builder.append("JSON ");

        if (isDistinct)
            builder.append("DISTINCT ");

        if (columnNames == null) {
            builder.append('*');
        } else {
            Utils.joinAndAppendNames(builder, codecRegistry, columnNames);
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

        if (perPartitionLimit != null) {
            builder.append(" PER PARTITION LIMIT ").append(perPartitionLimit);
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
     * Adds a {@code LIMIT} clause to this statement.
     *
     * @param limit the limit to set.
     * @return this statement.
     * @throws IllegalArgumentException if {@code limit <= 0}.
     * @throws IllegalStateException    if a {@code LIMIT} clause has already been
     *                                  provided.
     */
    public Select limit(int limit) {
        if (limit <= 0)
            throw new IllegalArgumentException("Invalid LIMIT value, must be strictly positive");

        if (this.limit != null)
            throw new IllegalStateException("A LIMIT value has already been provided");

        this.limit = limit;
        setDirty();
        return this;
    }

    /**
     * Adds a prepared {@code LIMIT} clause to this statement.
     *
     * @param marker the marker to use for the limit.
     * @return this statement.
     * @throws IllegalStateException if a {@code LIMIT} clause has already been
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
     * Adds a {@code PER PARTITION LIMIT} clause to this statement.
     * <p>
     * Note: support for {@code PER PARTITION LIMIT} clause is only available from
     * Cassandra 3.6 onwards.
     *
     * @param perPartitionLimit the limit to set per partition.
     * @return this statement.
     * @throws IllegalArgumentException if {@code perPartitionLimit <= 0}.
     * @throws IllegalStateException    if a {@code PER PARTITION LIMIT} clause has already been
     *                                  provided.
     * @throws IllegalStateException    if this statement is a {@code SELECT DISTINCT} statement.
     */
    public Select perPartitionLimit(int perPartitionLimit) {
        if (perPartitionLimit <= 0)
            throw new IllegalArgumentException("Invalid PER PARTITION LIMIT value, must be strictly positive");

        if (this.perPartitionLimit != null)
            throw new IllegalStateException("A PER PARTITION LIMIT value has already been provided");
        if (isDistinct)
            throw new IllegalStateException("PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries");

        this.perPartitionLimit = perPartitionLimit;
        setDirty();
        return this;
    }

    /**
     * Adds a prepared {@code PER PARTITION LIMIT} clause to this statement.
     * <p>
     * Note: support for {@code PER PARTITION LIMIT} clause is only available from
     * Cassandra 3.6 onwards.
     *
     * @param marker the marker to use for the limit per partition.
     * @return this statement.
     * @throws IllegalStateException if a {@code PER PARTITION LIMIT} clause has already been
     *                               provided.
     * @throws IllegalStateException if this statement is a {@code SELECT DISTINCT} statement.
     */
    public Select perPartitionLimit(BindMarker marker) {
        if (this.perPartitionLimit != null)
            throw new IllegalStateException("A PER PARTITION LIMIT value has already been provided");
        if (isDistinct)
            throw new IllegalStateException("PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries");

        this.perPartitionLimit = marker;
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
         * Adds a {@code LIMIT} clause to the {@code SELECT} statement this
         * {@code WHERE} clause if part of.
         *
         * @param limit the limit to set.
         * @return the {@code SELECT} statement this {@code WHERE} clause if part of.
         * @throws IllegalArgumentException if {@code limit <= 0}.
         * @throws IllegalStateException    if a {@code LIMIT} clause has already been
         *                                  provided.
         */
        public Select limit(int limit) {
            return statement.limit(limit);
        }

        /**
         * Adds a bind marker for the {@code LIMIT} clause to the {@code SELECT} statement this
         * {@code WHERE} clause if part of.
         *
         * @param limit the bind marker to use as limit.
         * @return the {@code SELECT} statement this {@code WHERE} clause if part of.
         * @throws IllegalStateException if a {@code LIMIT} clause has already been
         *                               provided.
         */
        public Select limit(BindMarker limit) {
            return statement.limit(limit);
        }

        /**
         * Adds a {@code PER PARTITION LIMIT} clause to the {@code SELECT} statement this
         * {@code WHERE} clause if part of.
         * <p>
         * Note: support for {@code PER PARTITION LIMIT} clause is only available from
         * Cassandra 3.6 onwards.
         *
         * @param perPartitionLimit the limit to set per partition.
         * @return the {@code SELECT} statement this {@code WHERE} clause if part of.
         * @throws IllegalArgumentException if {@code perPartitionLimit <= 0}.
         * @throws IllegalStateException    if a {@code PER PARTITION LIMIT} clause has already been
         *                                  provided.
         * @throws IllegalStateException    if this statement is a {@code SELECT DISTINCT} statement.
         */
        public Select perPartitionLimit(int perPartitionLimit) {
            return statement.perPartitionLimit(perPartitionLimit);
        }

        /**
         * Adds a bind marker for the {@code PER PARTITION LIMIT} clause to the {@code SELECT} statement this
         * {@code WHERE} clause if part of.
         * <p>
         * Note: support for {@code PER PARTITION LIMIT} clause is only available from
         * Cassandra 3.6 onwards.
         *
         * @param limit the bind marker to use as limit per partition.
         * @return the {@code SELECT} statement this {@code WHERE} clause if part of.
         * @throws IllegalStateException if a {@code PER PARTITION LIMIT} clause has already been
         *                               provided.
         * @throws IllegalStateException if this statement is a {@code SELECT DISTINCT} statement.
         */
        public Select perPartitionLimit(BindMarker limit) {
            return statement.perPartitionLimit(limit);
        }

    }

    /**
     * An in-construction SELECT statement.
     */
    public static class Builder {

        List<Object> columnNames;
        boolean isDistinct;
        boolean isJson;

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
         * Uses JSON selection.
         * <p>
         * Cassandra 2.2 introduced JSON support to SELECT statements:
         * the {@code JSON} keyword can be used to return each row as a single JSON encoded map.
         *
         * @return this in-build SELECT statement.
         * @see <a href="http://cassandra.apache.org/doc/cql3/CQL-2.2.html#json">JSON Support for CQL</a>
         * @see <a href="http://www.datastax.com/dev/blog/whats-new-in-cassandra-2-2-json-support">JSON Support in Cassandra 2.2</a>
         * @see <a href="https://docs.datastax.com/en/cql/3.3/cql/cql_using/useQueryJSON.html">Data retrieval using JSON</a>
         */
        public Builder json() {
            this.isJson = true;
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
            return new Select(keyspace, table, columnNames, isDistinct, isJson);
        }

        /**
         * Adds the table to select from.
         *
         * @param table the table to select from.
         * @return a newly built SELECT statement that selects from {@code table}.
         */
        public Select from(TableMetadata table) {
            return new Select(table, columnNames, isDistinct, isJson);
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
         * Uses JSON selection.
         * <p>
         * Cassandra 2.2 introduced JSON support to SELECT statements:
         * the {@code JSON} keyword can be used to return each row as a single JSON encoded map.
         *
         * @return this in-build SELECT statement.
         * @see <a href="http://cassandra.apache.org/doc/cql3/CQL-2.2.html#json">JSON Support for CQL</a>
         * @see <a href="http://www.datastax.com/dev/blog/whats-new-in-cassandra-2-2-json-support">JSON Support in Cassandra 2.2</a>
         * @see <a href="https://docs.datastax.com/en/cql/3.3/cql/cql_using/useQueryJSON.html">Data retrieval using JSON</a>
         */
        @Override
        public Selection json() {
            this.isJson = true;
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

        /**
         * Creates a cast of an expression to a given CQL type.
         *
         * @param column     the expression to cast. It can be a complex expression like a
         *                   {@link QueryBuilder#fcall(String, Object...) function call}.
         * @param targetType the target CQL type to cast to. Use static methods such as {@link DataType#text()}.
         * @return this in-build SELECT statement.
         */
        public SelectionOrAlias cast(Object column, DataType targetType) {
            // This method should be abstract like others here. But adding an abstract method is not binary-compatible,
            // so we add this dummy implementation to make Clirr happy.
            throw new UnsupportedOperationException("Not implemented. This should only happen if you've written your own implementation of Selection");
        }

        /**
         * Selects the provided raw expression.
         * <p/>
         * The provided string will be appended to the query as-is, without any form of escaping or quoting.
         *
         * @param rawString the raw expression to add.
         * @return this in-build SELECT statement
         */
        public SelectionOrAlias raw(String rawString) {
            // This method should be abstract like others here. But adding an abstract method is not binary-compatible,
            // so we add this dummy implementation to make Clirr happy.
            throw new UnsupportedOperationException("Not implemented. This should only happen if you've written your own implementation of Selection");
        }

        /**
         * Selects the provided path.
         * <p/>
         * All given path {@code segments} will be concatenated together with dots.
         * If any segment contains an identifier that needs quoting,
         * caller code is expected to call {@link QueryBuilder#quote(String)} prior to
         * invoking this method.
         * <p/>
         * This method is currently only useful when accessing individual fields of a
         * {@link com.datastax.driver.core.UserType user-defined type} (UDT),
         * which is only possible since CASSANDRA-7423.
         * <p/>
         * Note that currently nested UDT fields are not supported and
         * will be rejected by the server as a
         * {@link com.datastax.driver.core.exceptions.SyntaxError syntax error}.
         *
         * @param segments the segments of the path to create.
         * @return this in-build SELECT statement
         * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-7423">CASSANDRA-7423</a>
         */
        public SelectionOrAlias path(String... segments) {
            // This method should be abstract like others here. But adding an abstract method is not binary-compatible,
            // so we add this dummy implementation to make Clirr happy.
            throw new UnsupportedOperationException("Not implemented. This should only happen if you've written your own implementation of Selection");
        }

        /**
         * Creates a {@code toJson()} function call.
         * This is a shortcut for {@code fcall("toJson", QueryBuilder.column(name))}.
         * <p>
         * Support for JSON functions has been added in Cassandra 2.2.
         * The {@code toJson()} function is similar to {@code SELECT JSON} statements,
         * but applies to a single column value instead of the entire row,
         * and produces a JSON-encoded string representing the normal Cassandra column value.
         * <p>
         * It may only be used in the selection clause of a {@code SELECT} statement.
         *
         * @return the function call.
         * @see <a href="http://cassandra.apache.org/doc/cql3/CQL-2.2.html#json">JSON Support for CQL</a>
         * @see <a href="http://www.datastax.com/dev/blog/whats-new-in-cassandra-2-2-json-support">JSON Support in Cassandra 2.2</a>
         */
        public SelectionOrAlias toJson(String name) {
            // This method should be abstract like others here. But adding an abstract method is not binary-compatible,
            // so we add this dummy implementation to make Clirr happy.
            throw new UnsupportedOperationException("Not implemented. This should only happen if you've written your own implementation of Selection");
        }
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

        @Override
        public Builder all() {
            if (columnNames != null)
                throw new IllegalStateException(String.format("Some columns (%s) have already been selected.", columnNames));
            if (previousSelection != null)
                throw new IllegalStateException(String.format("Some columns ([%s]) have already been selected.", previousSelection));

            return (Builder) this;
        }

        @Override
        public Builder countAll() {
            if (columnNames != null)
                throw new IllegalStateException(String.format("Some columns (%s) have already been selected.", columnNames));
            if (previousSelection != null)
                throw new IllegalStateException(String.format("Some columns ([%s]) have already been selected.", previousSelection));

            columnNames = COUNT_ALL;
            return (Builder) this;
        }

        @Override
        public SelectionOrAlias column(String name) {
            return queueName(name);
        }

        @Override
        public SelectionOrAlias writeTime(String name) {
            return queueName(new Utils.FCall("writetime", new Utils.CName(name)));
        }

        @Override
        public SelectionOrAlias ttl(String name) {
            return queueName(new Utils.FCall("ttl", new Utils.CName(name)));
        }

        @Override
        public SelectionOrAlias fcall(String name, Object... parameters) {
            return queueName(new Utils.FCall(name, parameters));
        }

        @Override
        public SelectionOrAlias cast(Object column, DataType targetType) {
            return queueName(new Utils.Cast(column, targetType));
        }

        @Override
        public SelectionOrAlias raw(String rawString) {
            return queueName(QueryBuilder.raw(rawString));
        }

        @Override
        public SelectionOrAlias path(String... segments) {
            return queueName(QueryBuilder.path(segments));
        }

        @Override
        public SelectionOrAlias toJson(String name) {
            return queueName(new Utils.FCall("toJson", new Utils.CName(name)));
        }

        @Override
        public Select from(String keyspace, String table) {
            if (previousSelection != null)
                addName(previousSelection);
            previousSelection = null;
            return super.from(keyspace, table);
        }

        @Override
        public Select from(TableMetadata table) {
            if (previousSelection != null)
                addName(previousSelection);
            previousSelection = null;
            return super.from(table);
        }
    }
}
