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
import com.datastax.driver.core.querybuilder.Assignment.CounterAssignment;

import java.util.ArrayList;
import java.util.List;

/**
 * A built UPDATE statement.
 */
public class Update extends BuiltStatement {

    private final String table;
    private final Assignments assignments;
    private final Where where;
    private final Options usings;
    private final Conditions conditions;
    private boolean ifExists;

    Update(String keyspace, String table) {
        super(keyspace);
        this.table = table;
        this.assignments = new Assignments(this);
        this.where = new Where(this);
        this.usings = new Options(this);
        this.conditions = new Conditions(this);
        this.ifExists = false;
    }

    Update(TableMetadata table) {
        super(table);
        this.table = escapeId(table.getName());
        this.assignments = new Assignments(this);
        this.where = new Where(this);
        this.usings = new Options(this);
        this.conditions = new Conditions(this);
        this.ifExists = false;
    }

    @Override
    StringBuilder buildQueryString(List<Object> variables, CodecRegistry codecRegistry) {
        StringBuilder builder = new StringBuilder();

        builder.append("UPDATE ");
        if (keyspace != null)
            Utils.appendName(keyspace, builder).append('.');
        Utils.appendName(table, builder);

        if (!usings.usings.isEmpty()) {
            builder.append(" USING ");
            Utils.joinAndAppend(builder, codecRegistry, " AND ", usings.usings, variables);
        }

        if (!assignments.assignments.isEmpty()) {
            builder.append(" SET ");
            Utils.joinAndAppend(builder, codecRegistry, ",", assignments.assignments, variables);
        }

        if (!where.clauses.isEmpty()) {
            builder.append(" WHERE ");
            Utils.joinAndAppend(builder, codecRegistry, " AND ", where.clauses, variables);
        }

        if (!conditions.conditions.isEmpty()) {
            builder.append(" IF ");
            Utils.joinAndAppend(builder, codecRegistry, " AND ", conditions.conditions, variables);
        }

        if (ifExists) {
            builder.append(" IF EXISTS");
        }

        return builder;
    }

    /**
     * Adds an assignment to this UPDATE statement.
     * <p/>
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
            if (!hasNonIdempotentOps() && !Utils.isIdempotent(assignment))
                statement.setNonIdempotentOps();
            assignments.add(assignment);
            checkForBindMarkers(assignment);
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

        /**
         * Adds a condition to the UPDATE statement those assignments are part of.
         *
         * @param condition the condition to add.
         * @return the conditions for the UPDATE statement those assignments are part of.
         */
        public Conditions onlyIf(Clause condition) {
            return statement.onlyIf(condition);
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
        public Where and(Clause clause) {
            clauses.add(clause);
            statement.maybeAddRoutingKey(clause.name(), clause.firstValue());
            checkForBindMarkers(clause);
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

        /**
         * Adds a condition to the UPDATE statement this WHERE clause is part of.
         *
         * @param condition the condition to add.
         * @return the conditions for the UPDATE statement this WHERE clause is part of.
         */
        public Conditions onlyIf(Clause condition) {
            return statement.onlyIf(condition);
        }

        /**
         * Sets the 'IF EXISTS' option for the UPDATE statement this WHERE clause
         * is part of.
         * <p/>
         * <p>
         * An update with that option will report whether the statement actually
         * resulted in data being updated. The existence check and update are
         * done transactionally in the sense that if multiple clients attempt to
         * update a given row with this option, then at most one may succeed.
         * </p>
         * <p>
         * Please keep in mind that using this option has a non negligible
         * performance impact and should be avoided when possible.
         * </p>
         *
         * @return the UPDATE statement this WHERE clause is part of.
         */
        public IfExists ifExists() {
            statement.ifExists = true;
            return new IfExists(statement);
        }
    }

    /**
     * The options of a UPDATE statement.
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
            checkForBindMarkers(using);
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

        /**
         * Adds a condition to the UPDATE statement these options are part of.
         *
         * @param condition the condition to add.
         * @return the conditions for the UPDATE statement these options are part of.
         */
        public Conditions onlyIf(Clause condition) {
            return statement.onlyIf(condition);
        }
    }

    /**
     * Conditions for an UPDATE statement.
     * <p/>
     * When provided some conditions, an update will not apply unless the
     * provided conditions applies.
     * <p/>
     * Please keep in mind that provided conditions has a non negligible
     * performance impact and should be avoided when possible.
     */
    public static class Conditions extends BuiltStatement.ForwardingStatement<Update> {

        private final List<Clause> conditions = new ArrayList<Clause>();

        Conditions(Update statement) {
            super(statement);
        }

        /**
         * Adds the provided condition for the update.
         * <p/>
         * Note that while the query builder accept any type of {@code Clause}
         * as conditions, Cassandra currently only allow equality ones.
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
         * Adds an assignment to the UPDATE statement those conditions are part of.
         *
         * @param assignment the assignment to add.
         * @return the assignments of the UPDATE statement those conditions are part of.
         */
        public Assignments with(Assignment assignment) {
            return statement.with(assignment);
        }

        /**
         * Adds a where clause to the UPDATE statement these conditions are part of.
         *
         * @param clause clause to add.
         * @return the WHERE clause of the UPDATE statement these conditions are part of.
         */
        public Where where(Clause clause) {
            return statement.where(clause);
        }

        /**
         * Adds an option to the UPDATE statement these conditions are part of.
         *
         * @param using the using clause to add.
         * @return the options of the UPDATE statement these conditions are part of.
         */
        public Options using(Using using) {
            return statement.using(using);
        }

    }

    public static class IfExists extends BuiltStatement.ForwardingStatement<Update> {
        IfExists(Update statement) {
            super(statement);
        }
    }
}
