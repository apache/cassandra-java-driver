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
import java.util.List;

/**
 * A built DELETE statement.
 */
public class Delete extends BuiltStatement {

    private final String table;
    private final List<Selector> columns;
    private final Where where;
    private final Options usings;
    private final Conditions conditions;
    private boolean ifExists;

    Delete(String keyspace, String table, List<Selector> columns) {
        super(keyspace);
        this.table = table;
        this.columns = columns;
        this.where = new Where(this);
        this.usings = new Options(this);
        this.conditions = new Conditions(this);
    }

    Delete(TableMetadata table, List<Selector> columns) {
        super(table);
        this.table = escapeId(table.getName());
        this.columns = columns;
        this.where = new Where(this);
        this.usings = new Options(this);
        this.conditions = new Conditions(this);
    }

    @Override
    StringBuilder buildQueryString(List<Object> variables, CodecRegistry codecRegistry) {
        StringBuilder builder = new StringBuilder();

        builder.append("DELETE");
        if (!columns.isEmpty())
            Utils.joinAndAppend(builder.append(" "), codecRegistry, ",", columns, variables);

        builder.append(" FROM ");
        if (keyspace != null)
            Utils.appendName(keyspace, builder).append('.');
        Utils.appendName(table, builder);
        if (!usings.usings.isEmpty()) {
            builder.append(" USING ");
            Utils.joinAndAppend(builder, codecRegistry, " AND ", usings.usings, variables);
        }

        if (!where.clauses.isEmpty()) {
            builder.append(" WHERE ");
            Utils.joinAndAppend(builder, codecRegistry, " AND ", where.clauses, variables);
        }

        if (ifExists) {
            builder.append(" IF EXISTS ");
        }

        if (!conditions.conditions.isEmpty()) {
            builder.append(" IF ");
            Utils.joinAndAppend(builder, codecRegistry, " AND ", conditions.conditions, variables);
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
     * Adds a conditions clause (IF) to this statement.
     * <p/>
     * This is a shorter/more readable version for {@code onlyIf().and(condition)}.
     *
     * @param condition the condition to add.
     * @return the conditions of this query to which more conditions can be added.
     */
    public Conditions onlyIf(Clause condition) {
        return conditions.and(condition);
    }

    /**
     * Adds a conditions clause (IF) to this statement.
     *
     * @return the conditions of this query to which more conditions can be added.
     */
    public Conditions onlyIf() {
        return conditions;
    }

    /**
     * Adds a new options for this DELETE statement.
     *
     * @param using the option to add.
     * @return the options of this DELETE statement.
     */
    public Options using(Using using) {
        return usings.and(using);
    }

    /**
     * Returns the options for this DELETE statement.
     * <p/>
     * Chain this with {@link Options#and(Using)} to add options.
     *
     * @return the options of this DELETE statement.
     */
    public Options using() {
        return usings;
    }

    /**
     * Sets the 'IF EXISTS' option for this DELETE statement.
     * <p/>
     * <p>
     * A delete with that option will report whether the statement actually
     * resulted in data being deleted. The existence check and deletion are
     * done transactionally in the sense that if multiple clients attempt to
     * delete a given row with this option, then at most one may succeed.
     * </p>
     * <p>
     * Please keep in mind that using this option has a non negligible
     * performance impact and should be avoided when possible.
     * </p>
     *
     * @return this DELETE statement.
     */
    public Delete ifExists() {
        this.ifExists = true;
        return this;
    }

    /**
     * The WHERE clause of a DELETE statement.
     */
    public static class Where extends BuiltStatement.ForwardingStatement<Delete> {

        private final List<Clause> clauses = new ArrayList<Clause>();

        Where(Delete statement) {
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
         * Adds an option to the DELETE statement this WHERE clause is part of.
         *
         * @param using the using clause to add.
         * @return the options of the DELETE statement this WHERE clause is part of.
         */
        public Options using(Using using) {
            return statement.using(using);
        }

        /**
         * Sets the 'IF EXISTS' option for the DELETE statement this WHERE clause
         * is part of.
         * <p/>
         * <p>
         * A delete with that option will report whether the statement actually
         * resulted in data being deleted. The existence check and deletion are
         * done transactionally in the sense that if multiple clients attempt to
         * delete a given row with this option, then at most one may succeed.
         * </p>
         * <p>
         * Please keep in mind that using this option has a non negligible
         * performance impact and should be avoided when possible.
         * </p>
         *
         * @return the DELETE statement this WHERE clause is part of.
         */
        public Delete ifExists() {
            return statement.ifExists();
        }

        /**
         * Adds a condition to the DELETE statement this WHERE clause is part of.
         *
         * @param condition the condition to add.
         * @return the conditions for the DELETE statement this WHERE clause is part of.
         */
        public Conditions onlyIf(Clause condition) {
            return statement.onlyIf(condition);
        }
    }

    /**
     * The options of a DELETE statement.
     */
    public static class Options extends BuiltStatement.ForwardingStatement<Delete> {

        private final List<Using> usings = new ArrayList<Using>();

        Options(Delete statement) {
            super(statement);
        }

        /**
         * Adds the provided option.
         *
         * @param using a DELETE option.
         * @return this {@code Options} object.
         */
        public Options and(Using using) {
            usings.add(using);
            checkForBindMarkers(using);
            return this;
        }

        /**
         * Adds a where clause to the DELETE statement these options are part of.
         *
         * @param clause clause to add.
         * @return the WHERE clause of the DELETE statement these options are part of.
         */
        public Where where(Clause clause) {
            return statement.where(clause);
        }
    }

    /**
     * An in-construction DELETE statement.
     */
    public static class Builder {

        List<Selector> columns = new ArrayList<Selector>();

        Builder() {
        }

        Builder(String... columnNames) {
            for (String columnName : columnNames) {
                this.columns.add(new Selector(columnName));
            }
        }

        /**
         * Adds the table to delete from.
         *
         * @param table the name of the table to delete from.
         * @return a newly built DELETE statement that deletes from {@code table}.
         */
        public Delete from(String table) {
            return from(null, table);
        }

        /**
         * Adds the table to delete from.
         *
         * @param keyspace the name of the keyspace to delete from.
         * @param table    the name of the table to delete from.
         * @return a newly built DELETE statement that deletes from {@code keyspace.table}.
         */
        public Delete from(String keyspace, String table) {
            return new Delete(keyspace, table, columns);
        }

        /**
         * Adds the table to delete from.
         *
         * @param table the table to delete from.
         * @return a newly built DELETE statement that deletes from {@code table}.
         */
        public Delete from(TableMetadata table) {
            return new Delete(table, columns);
        }
    }

    /**
     * An column selection clause for an in-construction DELETE statement.
     */
    public static class Selection extends Builder {

        /**
         * Deletes all columns (i.e. "DELETE FROM ...")
         *
         * @return an in-build DELETE statement.
         * @throws IllegalStateException if some columns had already been selected for this builder.
         */
        public Builder all() {
            if (!columns.isEmpty())
                throw new IllegalStateException(String.format("Some columns (%s) have already been selected.", columns));

            return this;
        }

        /**
         * Deletes the provided column.
         *
         * @param columnName the column to select for deletion.
         * @return this in-build DELETE Selection
         */
        public Selection column(String columnName) {
            columns.add(new Selector(columnName));
            return this;
        }

        /**
         * Deletes the provided list element.
         *
         * @param columnName the name of the list column.
         * @param idx        the index of the element to delete.
         * @return this in-build DELETE Selection
         */
        public Selection listElt(String columnName, int idx) {
            columns.add(new CollectionElementSelector(columnName, idx));
            return this;
        }

        /**
         * Deletes the provided list element,
         * specified as a bind marker.
         *
         * @param columnName the name of the list column.
         * @param idx        the index of the element to delete.
         * @return this in-build DELETE Selection
         */
        public Selection listElt(String columnName, BindMarker idx) {
            columns.add(new CollectionElementSelector(columnName, idx));
            return this;
        }

        /**
         * Deletes the provided set element.
         *
         * @param columnName the name of the set column.
         * @param element    the element to delete.
         * @return this in-build DELETE Selection
         */
        public Selection setElt(String columnName, Object element) {
            columns.add(new CollectionElementSelector(columnName, element));
            return this;
        }

        /**
         * Deletes the provided set element,
         * specified as a bind marker.
         *
         * @param columnName the name of the set column.
         * @param element    the element to delete.
         * @return this in-build DELETE Selection
         */
        public Selection setElt(String columnName, BindMarker element) {
            columns.add(new CollectionElementSelector(columnName, element));
            return this;
        }

        /**
         * Deletes a map element given a key.
         *
         * @param columnName the name of the map column.
         * @param key        the key for the element to delete.
         * @return this in-build DELETE Selection
         */
        public Selection mapElt(String columnName, Object key) {
            columns.add(new CollectionElementSelector(columnName, key));
            return this;
        }
    }

    /**
     * A selector in a DELETE selection clause.
     * A selector can be either a column name,
     * a list element, a set element or a map entry.
     */
    static class Selector extends Utils.Appendeable {

        private final String columnName;

        Selector(String columnName) {
            this.columnName = columnName;
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> values, CodecRegistry codecRegistry) {
            Utils.appendName(columnName, sb);
        }

        @Override
        boolean containsBindMarker() {
            return false;
        }

        @Override
        public String toString() {
            return columnName;
        }
    }

    /**
     * A selector representing a list index, a set element or a map key in a DELETE selection clause.
     */
    static class CollectionElementSelector extends Selector {

        private final Object key;

        CollectionElementSelector(String columnName, Object key) {
            super(columnName);
            this.key = key;
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> values, CodecRegistry codecRegistry) {
            super.appendTo(sb, values, codecRegistry);
            sb.append('[');
            Utils.appendValue(key, codecRegistry, sb, values);
            sb.append(']');
        }

        @Override
        boolean containsBindMarker() {
            return Utils.containsBindMarker(key);
        }

    }

    /**
     * Conditions for a DELETE statement.
     * <p>
     * When provided some conditions, a deletion will not apply unless the
     * provided conditions applies.
     * </p>
     * <p>
     * Please keep in mind that provided conditions have a non negligible
     * performance impact and should be avoided when possible.
     * </p>
     */
    public static class Conditions extends BuiltStatement.ForwardingStatement<Delete> {

        private final List<Clause> conditions = new ArrayList<Clause>();

        Conditions(Delete statement) {
            super(statement);
        }

        /**
         * Adds the provided condition for the deletion.
         * <p/>
         * Note that while the query builder accept any type of {@code Clause}
         * as conditions, Cassandra currently only allows equality ones.
         *
         * @param condition the condition to add.
         * @return this {@code Conditions} clause.
         */
        public Conditions and(Clause condition) {
            conditions.add(condition);
            checkForBindMarkers(condition);
            return this;
        }

        /**
         * Adds a where clause to the DELETE statement these conditions are part of.
         *
         * @param clause clause to add.
         * @return the WHERE clause of the DELETE statement these conditions are part of.
         */
        public Where where(Clause clause) {
            return statement.where(clause);
        }

        /**
         * Adds an option to the DELETE statement these conditions are part of.
         *
         * @param using the using clause to add.
         * @return the options of the DELETE statement these conditions are part of.
         */
        public Options using(Using using) {
            return statement.using(using);
        }
    }
}
