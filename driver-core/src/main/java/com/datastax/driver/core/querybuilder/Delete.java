/*
 *      Copyright (C) 2012 DataStax Inc.
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

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.TableMetadata;

/**
 * A built DELETE statement.
 */
public class Delete extends BuiltStatement {

    private final String keyspace;
    private final String table;
    private final List<Object> columnNames;
    private final Where where;
    private final Options usings;

    Delete(String keyspace, String table, List<Object> columnNames) {
        super();
        this.keyspace = keyspace;
        this.table = table;
        this.columnNames = columnNames;
        this.where = new Where(this);
        this.usings = new Options(this);
    }

    Delete(TableMetadata table, List<Object> columnNames) {
        super(table);
        this.keyspace = table.getKeyspace().getName();
        this.table = table.getName();
        this.columnNames = columnNames;
        this.where = new Where(this);
        this.usings = new Options(this);
    }

    @Override
    protected StringBuilder buildQueryString() {
        StringBuilder builder = new StringBuilder();

        builder.append("DELETE ");
        if (columnNames != null)
            Utils.joinAndAppendNames(builder, ",", columnNames);

        builder.append(" FROM ");
        if (keyspace != null)
            Utils.appendName(keyspace, builder).append(".");
        Utils.appendName(table, builder);
        if (!usings.usings.isEmpty()) {
            builder.append(" USING ");
            Utils.joinAndAppend(builder, " AND ", usings.usings);
        }

        if (!where.clauses.isEmpty()) {
            builder.append(" WHERE ");
            Utils.joinAndAppend(builder, " AND ", where.clauses);
        }

        return builder;
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
     * Adds a new options for this DELETE statement.
     *
     * @param using the option to add.
     * @return the options of this DELETE statement.
     */
    public Options using(Using using) {
        return usings.and(using);
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
        public Where and(Clause clause)
        {
            clauses.add(clause);
            statement.maybeAddRoutingKey(clause.name(), clause.firstValue());
            setDirty();
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
            setDirty();
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

        protected List<Object> columnNames;

        protected Builder() {}

        Builder(List<Object> columnNames) {
            this.columnNames = columnNames;
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
         * @param table the name of the table to delete from.
         * @return a newly built DELETE statement that deletes from {@code keyspace.table}.
         */
        public Delete from(String keyspace, String table) {
            return new Delete(keyspace, table, columnNames);
        }

        /**
         * Adds the table to delete from.
         *
         * @param table the table to delete from.
         * @return a newly built DELETE statement that deletes from {@code table}.
         */
        public Delete from(TableMetadata table) {
            return new Delete(table, columnNames);
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
         *
         * @throws IllegalStateException if some columns had already been selected for this builder.
         */
        public Builder all() {
            if (columnNames != null)
                throw new IllegalStateException(String.format("Some columns (%s) have already been selected.", columnNames));

            return (Builder)this;
        }

        /**
         * Deletes the provided column.
         *
         * @param name the column name to select for deletion.
         * @return this in-build DELETE Selection
         */
        public Selection column(String name) {
            if (columnNames == null)
                columnNames = new ArrayList<Object>();

            columnNames.add(name);
            return this;
        }

        /**
         * Deletes the provided list element.
         *
         * @param columnName the name of the list column.
         * @param idx the index of the element to delete.
         * @return this in-build DELETE Selection
         */
        public Selection listElt(String columnName, int idx) {
            StringBuilder sb = new StringBuilder();
            Utils.appendName(columnName, sb);
            return column(sb.append("[").append(idx).append("]").toString());
        }

        /**
         * Deletes a map element given a key.
         *
         * @param columnName the name of the map column.
         * @param key the key for the element to delete.
         * @return this in-build DELETE Selection
         */
        public Selection mapElt(String columnName, Object key) {
            StringBuilder sb = new StringBuilder();
            Utils.appendName(columnName, sb);
            sb.append("[");
            Utils.appendFlatValue(key, sb);
            return column(sb.append("]").toString());
        }
    }
}
