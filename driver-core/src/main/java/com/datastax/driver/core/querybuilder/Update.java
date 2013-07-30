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
import com.datastax.driver.core.querybuilder.Assignment.CounterAssignment;

/**
 * A built UPDATE statement.
 */
public class Update extends BuiltStatement {

    private final String keyspace;
    private final String table;
    private final Assignments assignments;
    private final Where where;
    private final Options usings;

    Update(String keyspace, String table) {
        super();
        this.keyspace = keyspace;
        this.table = table;
        this.assignments = new Assignments(this);
        this.where = new Where(this);
        this.usings = new Options(this);
    }

    Update(TableMetadata table) {
        super(table);
        this.keyspace = table.getKeyspace().getName();
        this.table = table.getName();
        this.assignments = new Assignments(this);
        this.where = new Where(this);
        this.usings = new Options(this);
    }

    @Override
    protected StringBuilder buildQueryString() {
        StringBuilder builder = new StringBuilder();

        builder.append("UPDATE ");
        if (keyspace != null)
            Utils.appendName(keyspace, builder).append(".");
        Utils.appendName(table, builder);

        if (!usings.usings.isEmpty()) {
            builder.append(" USING ");
            Utils.joinAndAppend(builder, " AND ", usings.usings);
        }

        if (!assignments.assignments.isEmpty()) {
            builder.append(" SET ");
            Utils.joinAndAppend(builder, ",", assignments.assignments);
        }

        if (!where.clauses.isEmpty()) {
            builder.append(" WHERE ");
            Utils.joinAndAppend(builder, " AND ", where.clauses);
        }

        return builder;
    }

    /**
     * Adds an assignment to this UPDATE statement.
     *
     * This is a shorter/more readable version for {@code with().and(assignment)}.
     *
     * @param assignment the assignment to add.
     * @return the Assignments of this UPDATE statement.
     */
    public Assignments with(Assignment assignment) {
        return assignments.and(assignment);
    }

    /**
     * Returns the assignments of this UPDATE statement.
     *
     * @return the assignments of this UPDATE statement.
     */
    public Assignments with() {
        return assignments;
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
     * Adds a new options for this UPDATE statement.
     *
     * @param using the option to add.
     * @return the options of this UPDATE statement.
     */
    public Options using(Using using) {
        return usings.and(using);
    }

    /**
     * The assignments of an UPDATE statement.
     */
    public static class Assignments extends BuiltStatement.ForwardingStatement<Update> {

        private final List<Assignment> assignments = new ArrayList<Assignment>();

        Assignments(Update statement) {
            super(statement);
        }

        /**
         * Adds a new assignment for this UPDATE statement.
         *
         * @param assignment the new Assignment to add.
         * @return these Assignments.
         */
        public Assignments and(Assignment assignment) {
            statement.setCounterOp(assignment instanceof CounterAssignment);
            assignments.add(assignment);
            setDirty();
            return this;
        }

        /**
         * Adds a where clause to the UPDATE statement those assignments are part of.
         *
         * @param clause the clause to add.
         * @return the where clause of the UPDATE statement those assignments are part of.
         */
        public Where where(Clause clause) {
            return statement.where(clause);
        }

        /**
         * Adds an option to the UPDATE statement those assignments are part of.
         *
         * @param using the using clause to add.
         * @return the options of the UPDATE statement those assignments are part of.
         */
        public Options using(Using using) {
            return statement.using(using);
        }
    }

    /**
     * The WHERE clause of an UPDATE statement.
     */
    public static class Where extends BuiltStatement.ForwardingStatement<Update> {

        private final List<Clause> clauses = new ArrayList<Clause>();

        Where(Update statement) {
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
         * Adds an assignment to the UPDATE statement this WHERE clause is part of.
         *
         * @param assignment the assignment to add.
         * @return the assignments of the UPDATE statement this WHERE clause is part of.
         */
        public Assignments with(Assignment assignment) {
            return statement.with(assignment);
        }

        /**
         * Adds an option to the UPDATE statement this WHERE clause is part of.
         *
         * @param using the using clause to add.
         * @return the options of the UPDATE statement this WHERE clause is part of.
         */
        public Options using(Using using) {
            return statement.using(using);
        }
    }

    /**
     * The options of a UDPATE statement.
     */
    public static class Options extends BuiltStatement.ForwardingStatement<Update> {

        private final List<Using> usings = new ArrayList<Using>();

        Options(Update statement) {
            super(statement);
        }

        /**
         * Adds the provided option.
         *
         * @param using an UPDATE option.
         * @return this {@code Options} object.
         */
        public Options and(Using using) {
            usings.add(using);
            setDirty();
            return this;
        }

        /**
         * Adds an assignment to the UPDATE statement those options are part of.
         *
         * @param assignment the assignment to add.
         * @return the assignments of the UPDATE statement those options are part of.
         */
        public Assignments with(Assignment assignment) {
            return statement.with(assignment);
        }

        /**
         * Adds a where clause to the UPDATE statement these options are part of.
         *
         * @param clause clause to add.
         * @return the WHERE clause of the UPDATE statement these options are part of.
         */
        public Where where(Clause clause) {
            return statement.where(clause);
        }
    }
}
